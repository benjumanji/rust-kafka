use types::*;
use std::io::{IoResult};


macro_rules! kafka_datastructures {
    (
        $(
            struct $Name:ident {
                $($name:ident: $t:ty),+
            }
        )+) => {
        $(
            pub struct $Name {
                $(pub $name: $t),+
            }

            impl KafkaSerializable for $Name {
                fn encode(&self, writer: &mut Writer) -> IoResult<()> {
                    $(try!(self.$name.encode(writer)));+
                    Ok(())
                }

                fn decode(reader: &mut Reader) -> IoResult<$Name> {
                    Ok($Name {
                        $($name: try!(KafkaSerializable::decode(reader)),)+
                    })
                }

                #[inline]
                fn size(&self) -> i32 {
                    [$(self.$name.size()),+].iter().fold(0, |acc, element| acc + *element)
                }
            }
        )+
    };
}


kafka_datastructures! (
    struct MetadataRequest {
        topic_names: Vec<String>
    }

    struct Broker {
        node_id: i32,
        host: String,
        port: i32
    }

    struct TopicMetadata {
        topic_error_code: i16,
        topic_name: String,
        partition_metadatas: Vec<PartitionMetadata>
    }

    struct PartitionMetadata {
        partition_error_code: i16,
        partition_id: i32,
        leader: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>
    }

    struct Message {
        crc: i32,
        magic_byte: i8,
        attributes: i8,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>
    }

    struct MessageSetElement {
        offset: i64,
        message: WithSize<Message>
    }

    struct MessageSet {
        messages: Vec<MessageSetElement>
    }

    struct MetadataResponse {
        brokers: Vec<Broker>,
        topic_metadatas: Vec<TopicMetadata>
    }

    struct ProduceRequestData {
        partition: i32,
        message_set: WithSize<MessageSet>
    }

    struct ProduceRequestTopic {
        topic_name: String,
        datas: Vec<ProduceRequestData>
    }

    struct ProduceRequest {
        required_acks: i16,
        timeout: i32,
        produce_request_topics: Vec<ProduceRequestTopic>
    }
)
