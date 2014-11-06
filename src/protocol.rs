use types::*;
use std::io::{IoResult};


macro_rules! kafka_data {
    (
        struct $Name:ident {
            $($name:ident: $t:ty),+
        }) => {
        pub struct $Name {
            $(pub $name: $t),+
        }

        impl KafkaEncodable for $Name {
            fn encode(&self, writer: &mut Writer) -> IoResult<()> {
                $(try!(self.$name.encode(writer)))+;
                Ok(())
            }
        }

        impl KafkaDecodable for $Name {
            fn decode(reader: &mut Reader) -> IoResult<$Name> {
                Ok($Name {
                    $($name: try!(KafkaDecodable::decode(reader)),)+
                })
            }
        }
    };
}


kafka_data! (
    struct MetadataRequest {
        topic_names: Vec<String>
    }
)

kafka_data! (
    struct Broker {
        node_id: i32,
        host: String,
        port: i32
    }
)

kafka_data! (
    struct TopicMetadata {
        topic_error_code: i16,
        topic_name: String,
        partition_metadatas: Vec<PartitionMetadata>
    }
)

kafka_data! (
    struct PartitionMetadata {
        partition_error_code: i16,
        partition_id: i32,
        leader: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>
    }
)

kafka_data! (
    struct MetadataResponse {
        brokers: Vec<Broker>,
        topic_metadatas: Vec<TopicMetadata>
    }
)
