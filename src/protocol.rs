use std::io;
use std::io::{IoResult, IoError, InvalidInput};
use std::io::util::LimitReader;

pub trait KafkaSerializable {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()>;
    fn decode(reader: &mut io::Reader) -> IoResult<Self>;
    fn size(&self) -> i32;
}

#[deriving(Show, PartialEq, Eq)]
pub struct WithSize<T:KafkaSerializable>(pub T);

impl KafkaSerializable for i8 {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        writer.write_i8(*self)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<i8> {
        reader.read_i8()
    }

    #[inline]
    fn size(&self) -> i32 {
        1
    }
}

impl KafkaSerializable for i16 {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        writer.write_be_i16(*self)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<i16> {
        reader.read_be_i16()
    }

    #[inline]
    fn size(&self) -> i32 {
        2
    }
}

impl KafkaSerializable for i32 {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        writer.write_be_i32(*self)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<i32> {
        reader.read_be_i32()
    }

    #[inline]
    fn size(&self) -> i32 {
        4
    }
}

impl KafkaSerializable for i64 {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        writer.write_be_i64(*self)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<i64> {
        reader.read_be_i64()
    }

    #[inline]
    fn size(&self) -> i32 {
        8
    }
}

impl KafkaSerializable for String {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        try!((self.len() as i16).encode(writer));
        writer.write_str(self.as_slice())
    }

    fn decode(reader: &mut io::Reader) -> IoResult<String> {
        let size: i16 = try!(KafkaSerializable::decode(reader));
        let buffer = try!(reader.read_exact(size as uint));

        assert!(size >= 0);
        match String::from_utf8(buffer) {
            Ok(string) => Ok(string),
            Err(_) => Err(IoError{kind: InvalidInput, desc: "Problem decoding buffer as utf8", detail: None})
        }
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i16).size() + (self.as_bytes().len() as i32)
    }
}

impl KafkaSerializable for Option<String> {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        match *self {
            Some(ref string) => {
                try!((string.len() as i16).encode(writer));
                writer.write_str(string.as_slice())
            },
            None => (-1i16).encode(writer)
        }
    }

    fn decode(reader: &mut io::Reader) -> IoResult<Option<String>> {
        let size: i16 = try!(KafkaSerializable::decode(reader));

        assert!(size >= -1);
        if size == -1 {
            Ok(None)
        } else {
            let buffer = try!(reader.read_exact(size as uint));

            match String::from_utf8(buffer) {
                Ok(string) => Ok(Some(string)),
                Err(_) => Err(IoError{kind: InvalidInput, desc: "Problem decoding buffer as utf8", detail: None})
            }
        }
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i16).size() + match *self {
            Some(ref string) => {
                string.as_bytes().len() as i32
            },
            None => 0
        }
    }
}

impl <T:KafkaSerializable> KafkaSerializable for Vec<T> {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        try!((self.len() as i32).encode(writer));
        for element in self.iter() {
            try!(element.encode(writer))
        }
        Ok(())
    }

    fn decode(reader: &mut io::Reader) -> IoResult<Vec<T>> {
        let size: i32 = try!(KafkaSerializable::decode(reader));

        assert!(size >= 0);
        let mut result = Vec::with_capacity(size as uint);
        for _ in range(0, size) {
            result.push(try!(KafkaSerializable::decode(reader)))
        }
        Ok(result)
    }

    #[inline]
    fn size(&self) -> i32 {
        self.iter().fold((0i32).size(), |sum, ref element| sum + element.size())
    }
}

impl KafkaSerializable for Vec<u8> {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        try!((self.len() as i32).encode(writer));
        writer.write(self.as_slice())
    }

    fn decode(reader: &mut io::Reader) -> IoResult<Vec<u8>> {
        let size: i32 = try!(KafkaSerializable::decode(reader));

        assert!(size >= 0);
        reader.read_exact(size as uint)
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i32).size() + (self.len() as i32)
    }
}

impl KafkaSerializable for Option<Vec<u8>> {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        match *self {
            Some(ref vector) => {
                try!((vector.len() as i32).encode(writer));
                writer.write(vector.as_slice())
            },
            None => (-1i32).encode(writer)
        }
    }

    fn decode(reader: &mut io::Reader) -> IoResult<Option<Vec<u8>>> {
        let size: i32 = try!(KafkaSerializable::decode(reader));

        assert!(size >= -1);
        if size == -1 {
            Ok(None)
        } else {
            let vec = try!(reader.read_exact(size as uint));
            Ok(Some(vec))
        }
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i32).size() + match *self {
            Some(ref vector) => {
                vector.len() as i32
            },
            None => 0
        }
    }
}

impl <T:KafkaSerializable> KafkaSerializable for WithSize<T>  {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        try!(self.0.size().encode(writer));
        self.0.encode(writer)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<WithSize<T>> {
        let size: i32 = try!(KafkaSerializable::decode(reader));
        let mut limited_reader = LimitReader::new(reader, size as uint);
        let result = try!(KafkaSerializable::decode(&mut limited_reader));

        assert_eq!(limited_reader.limit(), 0);
        Ok(WithSize(result))
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i32).size() + self.0.size()
    }
}



#[deriving(Show, PartialEq, Eq)]
pub enum Error {
    Unknown = -1,
    NoError = 0,
    OffsetOutOfRange = 1,
    InvalidMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidMessageSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageSizeTooLarge = 10,
    StaleControllerEpochCode = 11,
    OffsetMetadataTooLargeCode = 12,
    OffsetsLoadInProgressCode = 14,
    ConsumerCoordinatorNotAvailableCode = 15,
    NotCoordinatorForConsumerCode = 16
}

impl FromPrimitive for Error {
    fn from_i64(n: i64) -> Option<Error> {
        match n {
            0 => Some(NoError),
            1 => Some(OffsetOutOfRange),
            2 => Some(InvalidMessage),
            3 => Some(UnknownTopicOrPartition),
            4 => Some(InvalidMessageSize),
            5 => Some(LeaderNotAvailable),
            6 => Some(NotLeaderForPartition),
            7 => Some(RequestTimedOut),
            8 => Some(BrokerNotAvailable),
            9 => Some(ReplicaNotAvailable),
            10 => Some(MessageSizeTooLarge),
            11 => Some(StaleControllerEpochCode),
            12 => Some(OffsetMetadataTooLargeCode),
            14 => Some(OffsetsLoadInProgressCode),
            15 => Some(ConsumerCoordinatorNotAvailableCode),
            16 => Some(NotCoordinatorForConsumerCode),
            -1 => Some(Unknown),
            _ => None
        }
    }

    fn from_u64(_: u64) -> Option<Error> {
        panic!("Can't convert unsigned integer to Error")
    }
}

#[test]
fn test_fromprimitive() {
    for &(n, expected) in [(0, Some(NoError)), (-1, Some(Unknown)), (20, None)].iter() {
        let error: Option<Error> = FromPrimitive::from_i64(n);
        assert_eq!(error, expected);
    }
}

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

    struct PartitionMetadata {
        error_code: i16,
        partition: i32,
        leader: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>
    }

    struct TopicMetadata {
        error_code: i16,
        name: String,
        partitions: Vec<PartitionMetadata>
    }

    struct MetadataResponse {
        brokers: Vec<Broker>,
        topics: Vec<TopicMetadata>
    }

    struct ProduceRequestPartition {
        partition: i32,
        message_set: WithSize<MessageSet>
    }

    struct ProduceRequestTopic {
        name: String,
        partitions: Vec<ProduceRequestPartition>
    }

    struct ProduceRequest {
        required_acks: i16,
        timeout: i32,
        topics: Vec<ProduceRequestTopic>
    }

    struct ProduceResponsePartition {
        partition: i32,
        error_code: i16,
        offset: i64
    }

    struct ProduceResponseTopic {
        name: String,
        partitions: Vec<ProduceResponsePartition>
    }

    struct ProduceResponse {
        topics: Vec<ProduceResponseTopic>
    }

    struct OffsetRequestPartition {
        partition: i32,
        time: i64,
        max_number_of_offsets: i32
    }

    struct OffsetRequestTopic {
        name: String,
        partitions: Vec<OffsetRequestPartition>
    }

    struct OffsetRequest {
        replica_id: i32,
        requests: Vec<OffsetRequestTopic>
    }

    struct PartitionOffset {
        partition: i32,
        error_code: i16,
        offset: i64
    }

    struct OffsetResponseTopic {
        name: String,
        partitions: Vec<PartitionOffset>
    }

    struct OffsetResponse {
        responses: Vec<OffsetResponseTopic>
    }

    struct FetchRequestPartition {
        partition: i32,
        fetch_offset: i64,
        max_bytes: i32
    }

    struct FetchRequestTopic {
        name: String,
        partitions: Vec<FetchRequestPartition>
    }

    struct FetchRequest {
        replica_id: i32,
        max_wait_time: i32,
        min_bytes: i32,
        elements: Vec<FetchRequestTopic>
    }

    struct FetchResponsePartition {
        partition: i32,
        error_code: i16,
        highwater_mark_offset: i64,
        messages: WithSize<MessageSet>
    }

    struct FetchResponseTopic {
        name: String,
        partitions: Vec<FetchResponsePartition>
    }

    struct FetchResponse {
        topics: Vec<FetchResponseTopic>
    }

    struct ConsumerMetadataRequest {
        group: String
    }

    struct ConsumerMetadataResponse {
        error_code: i16,
        coordinator_id: i32,
        coordinator_host: String,
        coordinator_port: i32
    }

    struct OffsetCommitRequestPartition {
        partition: i32,
        offset: i64,
        timestamp: i64,
        metadata: String
    }

    struct OffsetCommitRequestTopic {
        name: String,
        partitions: Vec<OffsetCommitRequestPartition>
    }

    struct OffsetCommitRequest {
        consumer_group: String,
        topics: Vec<OffsetCommitRequestTopic>
    }

    struct OffsetCommitResponseTopic {
        name: String,
        partitions: Vec<i32>
    }

    struct OffsetCommitResponse {
        topics: Vec<OffsetCommitResponseTopic>
    }

    struct OffsetFetchRequestTopic {
        name: String,
        partitions: Vec<i32>
    }

    struct OffsetFetchRequest {
        consumer_group: String,
        topics: OffsetFetchRequestTopic
    }

    struct OffsetFetchResponsePartition {
        partition: i32,
        offset: i64,
        metadata: String,
        error_code: i16
    }

    struct OffsetFetchResponseTopic {
        name: String,
        partitions: Vec<OffsetFetchResponsePartition>
    }

    struct OffsetFetchResponse {
        topics: Vec<OffsetFetchResponseTopic>
    }
)

pub trait Request: KafkaSerializable {
    fn api_key(_: Option<Self>) -> i16;
}

impl Request for ProduceRequest {
    fn api_key(_: Option<ProduceRequest>) -> i16 { 0 }
}

impl Request for FetchRequest {
    fn api_key(_: Option<FetchRequest>) -> i16 { 1 }
}

impl Request for OffsetRequest {
    fn api_key(_: Option<OffsetRequest>) -> i16 { 2 }
}

impl Request for MetadataRequest {
    fn api_key(_: Option<MetadataRequest>) -> i16 { 3 }
}

impl Request for OffsetCommitRequest {
    fn api_key(_: Option<OffsetCommitRequest>) -> i16 { 8 }
}

impl Request for OffsetFetchRequest {
    fn api_key(_: Option<OffsetFetchRequest>) -> i16 { 9 }
}

impl Request for ConsumerMetadataRequest {
    fn api_key(_: Option<ConsumerMetadataRequest>) -> i16 { 10 }
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

impl Response for ProduceResponse {}
impl Response for FetchResponse {}
impl Response for OffsetResponse {}
impl Response for MetadataResponse {}
impl Response for OffsetFetchResponse {}
impl Response for OffsetCommitResponse {}
impl Response for ConsumerMetadataResponse {}

#[deriving(Show, PartialEq, Eq)]
pub struct ResponseMessage<T:Response> {
    pub correlation_id: i32,
    pub response: T
}

impl <T:Response> KafkaSerializable for ResponseMessage<T> {
    fn encode(&self, writer: &mut io::Writer) -> IoResult<()> {
        try!(self.correlation_id.encode(writer));
        self.response.encode(writer)
    }

    fn decode(reader: &mut io::Reader) -> IoResult<ResponseMessage<T>> {
        Ok(
            ResponseMessage{
                correlation_id: try!(KafkaSerializable::decode(reader)),
                response: try!(KafkaSerializable::decode(reader))
            }
        )
    }

    #[inline]
    fn size(&self) -> i32 {
        (0i32).size() + self.response.size()
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
    use std::fmt;
    use std::io::{MemWriter, MemReader};
    use super::*;

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

    fn write_read_test<T:KafkaSerializable + Eq + fmt::Show>(input: T) {
        let mut writer = MemWriter::new();
        input.encode(&mut writer).ok().unwrap();
        assert_eq!(writer.get_ref().len() as i32, input.size());
        let mut reader = MemReader::new(writer.unwrap());
        let result = KafkaSerializable::decode(&mut reader).ok().unwrap();
        assert!(reader.eof());
        assert_eq!(input, result);
    }

    #[test]
    fn test_i8() {
        for i in range(core::i8::MIN, core::i8::MAX) {
            write_read_test(i);
        }
    }

    #[test]
    fn test_i16() {
        for i in range(-10, 10i16) {
            write_read_test(i);
        }
    }

    #[test]
    fn test_i32() {
        for i in range(-10, 10i32) {
            write_read_test(i);
        }
    }

    #[test]
    fn test_i64() {
        for i in range(-10, 10i64) {
            write_read_test(i);
        }
    }

    #[test]
    fn test_string() {
        write_read_test(String::from_str("Interesting"));
    }

    #[test]
    fn test_option_string() {
        write_read_test(Some(String::from_str("Interesting")));
        let none_test: Option<String> = None;
        write_read_test(none_test);
    }

    #[test]
    fn test_vec() {
        write_read_test(vec![-1, 0, 1, 2, 3, 4, 5i16]);
    }

    #[test]
    fn test_vec_u8() {
        write_read_test(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10u8]);
    }

    #[test]
    fn test_option_vec_u8() {
        write_read_test(Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10u8]));
        let none_test: Option<Vec<u8>> = None;
        write_read_test(none_test);
    }

    #[test]
    fn test_option_withsize() {
        write_read_test(WithSize(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10u8]));
    }
}
