use std::io::{Writer, Reader, IoResult, IoError, InvalidInput};

pub trait KafkaEncodable {
    fn encode(&self, writer: &mut Writer) -> IoResult<()>;
}

pub trait KafkaDecodable {
    fn decode(reader: &mut Reader) -> IoResult<Self>;
}


impl KafkaEncodable for i8 {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_i8(*self)
    }
}

impl KafkaDecodable for i8 {
    fn decode(reader: &mut Reader) -> IoResult<i8> {
        reader.read_i8()
    }
}

impl KafkaEncodable for i16 {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i16(*self)
    }
}

impl KafkaDecodable for i16 {
    fn decode(reader: &mut Reader) -> IoResult<i16> {
        reader.read_be_i16()
    }
}

impl KafkaEncodable for i32 {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i32(*self)
    }
}

impl KafkaDecodable for i32 {
    fn decode(reader: &mut Reader) -> IoResult<i32> {
        reader.read_be_i32()
    }
}

impl KafkaEncodable for i64 {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i64(*self)
    }
}

impl KafkaDecodable for i64 {
    fn decode(reader: &mut Reader) -> IoResult<i64> {
        reader.read_be_i64()
    }
}

impl <'a>KafkaEncodable for &'a str {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        try!((self.len() as i16).encode(writer));
        writer.write_str(*self)
    }
}

impl KafkaEncodable for String {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        try!((self.len() as i16).encode(writer));
        writer.write_str(self.as_slice())
    }
}

impl KafkaDecodable for String {
    fn decode(reader: &mut Reader) -> IoResult<String> {
        let size: i16 = try!(KafkaDecodable::decode(reader));
        let buffer = try!(reader.read_exact(size as uint));

        match String::from_utf8(buffer) {
            Ok(string) => Ok(string),
            Err(_) => Err(IoError{kind: InvalidInput, desc: "Problem decoding buffer as utf8", detail: None})
        }
    }
}

impl KafkaEncodable for Option<String> {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        match *self {
            Some(ref string) => {
                try!((string.len() as i16).encode(writer));
                writer.write_str(string.as_slice())
            },
            None => (-1i16).encode(writer)
        }
    }
}

impl KafkaDecodable for Option<String> {
    fn decode(reader: &mut Reader) -> IoResult<Option<String>> {
        let size: i16 = try!(KafkaDecodable::decode(reader));

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
}

impl <T:KafkaEncodable> KafkaEncodable for Vec<T> {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        try!((self.len() as i32).encode(writer));
        for element in self.iter() {
            try!(element.encode(writer))
        }
        Ok(())
    }
}

impl <T:KafkaDecodable> KafkaDecodable for Vec<T> {
    fn decode(reader: &mut Reader) -> IoResult<Vec<T>> {
        let size: i32 = try!(KafkaDecodable::decode(reader));
        let mut result = Vec::with_capacity(size as uint);
        for _ in range(0, size) {
            result.push(try!(KafkaDecodable::decode(reader)))
        }
        Ok(result)
    }
}

impl KafkaEncodable for Vec<u8> {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        try!((self.len() as i32).encode(writer));
        writer.write(self.as_slice())
    }
}

impl KafkaDecodable for Vec<u8> {
    fn decode(reader: &mut Reader) -> IoResult<Vec<u8>> {
        let size: i32 = try!(KafkaDecodable::decode(reader));
        reader.read_exact(size as uint)
    }
}

impl KafkaEncodable for Option<Vec<u8>> {
    fn encode(&self, writer: &mut Writer) -> IoResult<()> {
        match *self {
            Some(ref vector) => {
                try!((vector.len() as i32).encode(writer));
                writer.write(vector.as_slice())
            },
            None => (-1i32).encode(writer)
        }
    }
}

impl KafkaDecodable for Option<Vec<u8>> {
    fn decode(reader: &mut Reader) -> IoResult<Option<Vec<u8>>> {
        let size: i32 = try!(KafkaDecodable::decode(reader));

        if size == -1 {
            Ok(None)
        } else {
            let vec = try!(reader.read_exact(size as uint));
            Ok(Some(vec))
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate test;
    extern crate core;
    use std;
    use super::*;
    use std::io::{MemWriter, MemReader};

    fn write_read_test<T:KafkaEncodable + KafkaDecodable + Eq + std::fmt::Show>(input: T) {
        let mut writer = MemWriter::new();
        input.encode(&mut writer).ok().unwrap();
        let mut reader = MemReader::new(writer.unwrap());
        let result = KafkaDecodable::decode(&mut reader).ok().unwrap();
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
}
