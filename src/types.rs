use std::io::IoError;
use std::error;

#[deriving(PartialEq, Eq, Clone, Show)]
pub enum ErrorKind {
    MalformedRequestError,
    MalformedResponseError,
    InternalIoError(IoError),
}

#[deriving(PartialEq, Eq, Clone, Show)]
pub struct KafkaError {
    pub kind: ErrorKind,
    pub desc: &'static str,
    pub detail: Option<String>,
}

impl error::FromError<IoError> for KafkaError {
    fn from_error(err: IoError) -> KafkaError {
        KafkaError {
            kind: InternalIoError(err),
            desc: "An internal IO error ocurred.",
            detail: None
        }
    }
}

impl error::FromError<(ErrorKind, &'static str)> for KafkaError {
    fn from_error((kind, desc): (ErrorKind, &'static str)) -> KafkaError {
        KafkaError {
            kind: kind,
            desc: desc,
            detail: None,
        }
    }
}

impl error::Error for KafkaError {
    fn description(&self) -> &str {
        match self.kind {
            InternalIoError(ref err) => err.desc,
            _ => self.desc,
        }
    }

    fn detail(&self) -> Option<String> {
        match self.kind {
            InternalIoError(ref err) => err.detail.clone(),
            _ => self.detail.clone(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self.kind {
            InternalIoError(ref err) => Some(err as &error::Error),
            _ => None,
        }
    }
}

pub type KafkaResult<T> = Result<T, KafkaError>;
