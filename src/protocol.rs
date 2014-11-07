use types::*;
use std::io;
use std::io::{IoResult};
use std::io::util::LimitReader;


macro_rules! kafka_datastructures {
    (
        $(
            struct $Name:ident {
                $($name:ident: $t:ty),+
            }
        )+) => {
        $(
            #[deriving(Show, PartialEq, Eq)]
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

pub trait Request: KafkaSerializable {
    fn api_key(_: Option<Self>) -> i16;
}

impl Request for MetadataRequest {
    fn api_key(_: Option<MetadataRequest>) -> i16 { 3 }
}

#[deriving(Show, PartialEq, Eq)]
pub struct RequestMessage<T:Request> {
    // api_key: i16,
    // api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    pub request_message: T
}

impl <T:Request> KafkaSerializable for RequestMessage<T> {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        try!(Request::api_key(None::<T>).encode(writer));
        try!((0i16).encode(writer)); // Currently the only API version is 0
        try!(self.correlation_id.encode(writer));
        try!(self.client_id.encode(writer));
        self.request_message.encode(writer)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<RequestMessage<T>> {
        let api_key: i16 = try!(KafkaSerializable::decode(reader));
        assert_eq!(api_key, Request::api_key(None::<T>));
        let api_version: i16 = try!(KafkaSerializable::decode(reader));
        assert_eq!(api_version, 0);
        Ok(
            RequestMessage{
                correlation_id: try!(KafkaSerializable::decode(reader)),
                client_id: try!(KafkaSerializable::decode(reader)),
                request_message: try!(KafkaSerializable::decode(reader))
            }
        )
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i16).size() + (0i16).size() + (0i32).size() + self.client_id.size() + self.request_message.size()
    }
}

pub trait Response: KafkaSerializable {}

impl Response for MetadataResponse {}

#[deriving(Show, PartialEq, Eq)]
pub struct ResponseMessage<T:Response> {
    pub correlation_id: i32,
    pub response_message: T
}

impl <T:Response> KafkaSerializable for ResponseMessage<T> {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        try!(self.correlation_id.encode(writer));
        self.response_message.encode(writer)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<ResponseMessage<T>> {
        Ok(
            ResponseMessage{
                correlation_id: try!(KafkaSerializable::decode(reader)),
                response_message: try!(KafkaSerializable::decode(reader))
            }
        )
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i32).size() + self.response_message.size()
    }
}

pub trait IsRequestOrResponse: KafkaSerializable {}
impl <T:Response> IsRequestOrResponse for ResponseMessage<T> {}
impl <T:Request> IsRequestOrResponse for RequestMessage<T> {}

#[deriving(Show, PartialEq, Eq)]
pub struct RequestOrResponse<T:IsRequestOrResponse>(pub T);

impl <T:IsRequestOrResponse> KafkaSerializable for RequestOrResponse<T>  {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        try!(self.0.size().encode(writer));
        self.0.encode(writer)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<RequestOrResponse<T>> {
        let size: i32 = try!(KafkaSerializable::decode(reader));
        let mut limited_reader = LimitReader::new(reader, size as uint);
        let result = try!(KafkaSerializable::decode(&mut limited_reader));

        assert_eq!(limited_reader.limit(), 0);

        Ok(RequestOrResponse(result))
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i32).size() + self.0.size()
    }
}

#[cfg(test)]
mod tests {
    extern crate test;
    extern crate core;
    use super::*;
    use types::KafkaSerializable;
    use std::io::MemWriter;


    #[test]
    fn test_full_metadata_request() {
        let mut writer = MemWriter::new();

        let request = RequestOrResponse(RequestMessage {
                correlation_id: 0,
                client_id: String::from_str("Client"),
                request_message: MetadataRequest{
                    topic_names: vec![String::from_str("test")]
                }
            }
        );

        request.encode(&mut writer).ok().unwrap();

        let expected = [
            0x00, 0x00, 0x00, 26,
            0x00,    3, // ApiKey
            0x00, 0x00, // Api Version
            0x00, 0x00, 0x00, 0x00, // Correlation ID
            0x00,    6,  'C' as u8,  'l' as u8, 'i' as u8, 'e' as u8, 'n' as u8, 't' as u8,
            0x00, 0x00, 0x00,    1,
            0x00,    4,  't' as u8,  'e' as u8, 's' as u8, 't' as u8
        ];

        assert_eq!(expected.as_slice(), writer.get_ref());
    }
}
