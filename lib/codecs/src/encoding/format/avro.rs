use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::Encoder;
use vector_config::configurable_component;
use vector_core::{config::DataType, event::Event, schema};

use crate::encoding::BuildError;

/// Response shape returned by `GET /subjects/{subject}/versions/latest` on a
/// Confluent Schema Registry.
#[derive(Deserialize)]
struct SchemaRegistryResponse {
    id: u32,
    schema: String,
}

/// Config used to build a `AvroSerializer`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AvroSerializerConfig {
    /// Options for the Avro serializer.
    pub avro: AvroSerializerOptions,
}

impl AvroSerializerConfig {
    /// Creates a new `AvroSerializerConfig`.
    pub const fn new(options: AvroSerializerOptions) -> Self {
        Self { avro: options }
    }

    /// Build the `AvroSerializer` from this configuration.
    pub fn build(&self) -> Result<AvroSerializer, BuildError> {
        match (&self.avro.schema_registry, &self.avro.schema) {
            (Some(registry), _) => {
                let url = format!(
                    "{}/subjects/{}/versions/latest",
                    registry.url.trim_end_matches('/'),
                    registry.subject
                );
                let body: SchemaRegistryResponse = ureq::get(&url)
                    .call()
                    .map_err(|e| format!("Failed to fetch schema from registry at {url}: {e}"))?
                    .body_mut()
                    .read_json()
                    .map_err(|e| format!("Failed to parse schema registry response: {e}"))?;
                let schema = apache_avro::Schema::parse_str(&body.schema)
                    .map_err(|e| format!("Failed to parse Avro schema from registry: {e}"))?;
                Ok(AvroSerializer {
                    schema,
                    schema_id: Some(body.id),
                })
            }
            (None, Some(schema_str)) => {
                let schema = apache_avro::Schema::parse_str(schema_str)
                    .map_err(|error| format!("Failed building Avro serializer: {error}"))?;
                Ok(AvroSerializer {
                    schema,
                    schema_id: None,
                })
            }
            (None, None) => Err(
                "Avro serializer requires either `avro.schema` or `avro.schema_registry` \
                 to be configured"
                    .into(),
            ),
        }
    }

    /// The data type of events that are accepted by `AvroSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        // TODO: Convert the Avro schema to a vector schema requirement.
        schema::Requirement::empty()
    }
}

/// Apache Avro serializer options.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct AvroSerializerOptions {
    /// The Avro schema as an inline JSON string.
    ///
    /// Either this or `schema_registry` must be configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[configurable(metadata(
        docs::examples = r#"{ "type": "record", "name": "log", "fields": [{ "name": "message", "type": "string" }] }"#
    ))]
    #[configurable(metadata(docs::human_name = "Schema JSON"))]
    pub schema: Option<String>,

    /// Confluent Schema Registry configuration.
    ///
    /// When set, the schema is fetched from the registry at startup and messages
    /// are encoded with the Confluent wire format: a 5-byte header consisting of
    /// a magic byte (`0x00`) followed by the 4-byte big-endian schema ID.
    ///
    /// Either this or `schema` must be configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<SchemaRegistryConfig>,
}

/// Confluent Schema Registry configuration for Avro encoding.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct SchemaRegistryConfig {
    /// The base URL of the Confluent Schema Registry.
    #[configurable(metadata(docs::examples = "http://schema-registry:8081"))]
    pub url: String,

    /// The subject name to look up in the registry.
    ///
    /// Typically `<topic-name>-value` following the TopicNameStrategy convention.
    #[configurable(metadata(docs::examples = "my-topic-value"))]
    pub subject: String,
}

/// Serializer that converts an `Event` to bytes using the Apache Avro format.
///
/// When built with a `schema_registry` configuration the output is prefixed
/// with the Confluent wire-format header so that Confluent-compatible consumers
/// can locate the correct schema for deserialization.
#[derive(Debug, Clone)]
pub struct AvroSerializer {
    schema: apache_avro::Schema,
    /// When `Some`, each encoded message is prefixed with the Confluent wire
    /// format header: `\x00` (magic byte) + 4-byte big-endian schema ID.
    schema_id: Option<u32>,
}

impl AvroSerializer {
    /// Creates a new `AvroSerializer` with an inline schema and no Confluent header.
    pub const fn new(schema: apache_avro::Schema) -> Self {
        Self {
            schema,
            schema_id: None,
        }
    }
}

impl Encoder<Event> for AvroSerializer {
    type Error = vector_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let log = event.into_log();
        let value = apache_avro::to_value(log)?;
        let value = value.resolve(&self.schema)?;
        let bytes = apache_avro::to_avro_datum(&self.schema, value)?;
        if let Some(schema_id) = self.schema_id {
            buffer.put_u8(0x00);
            buffer.put_u32(schema_id);
        }
        buffer.put_slice(&bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use indoc::indoc;
    use vector_core::event::{LogEvent, Value};
    use vrl::btreemap;

    use super::*;

    #[test]
    fn serialize_avro() {
        let event = Event::Log(LogEvent::from(btreemap! {
            "foo" => Value::from("bar")
        }));
        let schema = indoc! {r#"
            {
                "type": "record",
                "name": "Log",
                "fields": [
                    {
                        "name": "foo",
                        "type": ["string"]
                    }
                ]
            }
        "#}
        .to_owned();
        let config = AvroSerializerConfig::new(AvroSerializerOptions {
            schema: Some(schema),
            schema_registry: None,
        });
        let mut serializer = config.build().unwrap();
        let mut bytes = BytesMut::new();

        serializer.encode(event, &mut bytes).unwrap();

        assert_eq!(bytes.freeze(), b"\0\x06bar".as_slice());
    }

    #[test]
    fn serialize_avro_confluent_header() {
        let event = Event::Log(LogEvent::from(btreemap! {
            "foo" => Value::from("bar")
        }));
        let schema_str = indoc! {r#"
            {
                "type": "record",
                "name": "Log",
                "fields": [
                    {
                        "name": "foo",
                        "type": ["string"]
                    }
                ]
            }
        "#}
        .to_owned();
        let schema = apache_avro::Schema::parse_str(&schema_str).unwrap();
        let mut serializer = AvroSerializer {
            schema,
            schema_id: Some(42),
        };
        let mut bytes = BytesMut::new();

        serializer.encode(event, &mut bytes).unwrap();

        // magic byte (0x00) + schema ID 42 as big-endian u32 (0x0000002a) + avro payload
        assert_eq!(&bytes[0..5], b"\x00\x00\x00\x00\x2a");
        assert_eq!(&bytes[5..], b"\0\x06bar");
    }
}
