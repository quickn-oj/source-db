use std::fs::{create_dir, File, OpenOptions};
use std::path::PathBuf;

use std::io::prelude::*;
use std::io::{self, BufWriter, SeekFrom};

use bincode::{deserialize, serialize_into};
use compress::{entropy::ari, rle};
use serde::{Deserialize, Serialize};

pub const MASTER_DB: &'static str = "sources.qsdb";
pub const DICTIONARY: &'static str = "dictionary";
pub const DICTIONARY0: &'static str = "0.qsdd";
pub const BYTES_HEDAER: usize = 11;
pub const BYTES_BLOCK: usize = 16;
pub const BYTES_DICTIONARY_HEADER: usize = 8;
pub const BYTES_DICTIONARY_BLOCK: usize = 16;
pub const QSDB_REVERSION: u16 = 1;
pub const DEFAULT_EXP: u8 = 4;
pub const DEFAULT_HEADER: Header = Header {
    reversion: QSDB_REVERSION,
    divisor_exp: DEFAULT_EXP,
    len: 0,
};

#[derive(Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq)]
pub struct Header {
    reversion: u16,
    divisor_exp: u8,
    len: u64,
}

// Dynamic allocation
#[derive(Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq)]
pub struct Block {
    nth: u64,
    len: u64,
    // Other field is code: Vec<u8>
}

#[derive(Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq)]
pub struct DictionaryHeader {
    len: u64,
}

#[derive(Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq)]
pub struct DictionaryBlock {
    nth: u64,
    offset: u64,
}

pub enum Mode {
    Create,
    Modification,
}

/// # DBFile structure
/// It manages source code(s) by segment tree
/// - sources.qsdb
/// - dictionary
/// -- 0.qsdd
/// -- 1.qsdd
/// -- ...
/// -- n.qsdd
/// # Operation
/// - push(source: Vec<u8>) : costs O(lgn)
#[derive(Clone)]
pub struct DBFile {
    source_db_root: PathBuf,
    header: Header,
}

impl DBFile {
    pub fn new(source_db_root: PathBuf, exp_wrapped: Option<u8>) -> io::Result<Self> {
        let mut header: Header = DEFAULT_HEADER;
        if let Some(exp) = exp_wrapped {
            header.divisor_exp = exp;
        }
        Self::inner_write_header(source_db_root.clone(), header, Mode::Create)?;

        create_dir(source_db_root.join(DICTIONARY))?;
        Self::inner_write_dict_header(
            0,
            source_db_root.clone(),
            DictionaryHeader { len: 0 },
            Mode::Create,
        )?;

        Ok(Self {
            source_db_root: source_db_root,
            header: header,
        })
    }

    pub fn open(source_db_root: PathBuf) -> io::Result<Self> {
        Ok(Self {
            source_db_root: source_db_root.clone(),
            header: Self::inner_read_header(source_db_root.clone())?,
        })
    }

    pub fn inner_read_header(source_db_root: PathBuf) -> io::Result<Header> {
        let mut db_file = File::open(source_db_root.join(MASTER_DB))?;
        let mut header_buf: [u8; BYTES_HEDAER] = [0; BYTES_HEDAER];
        db_file.read_exact(&mut header_buf)?;
        let header: Header = deserialize(&header_buf).unwrap();
        Ok(header)
    }

    pub fn inner_write_header(
        source_db_root: PathBuf,
        header: Header,
        mode: Mode,
    ) -> io::Result<()> {
        let mut db_file = match mode {
            Mode::Modification => OpenOptions::new()
                .write(true)
                .open(source_db_root.join(MASTER_DB))?,
            _ => File::create(source_db_root.join(MASTER_DB))?,
        };
        db_file.seek(SeekFrom::Start(0))?;
        serialize_into(&mut db_file, &header).unwrap();
        db_file.sync_all()?;
        Ok(())
    }

    pub fn inner_read_dict_header(
        idx: u64,
        source_db_root: PathBuf,
    ) -> io::Result<DictionaryHeader> {
        let mut dict_file = File::open(
            source_db_root
                .join(DICTIONARY)
                .join(format!("{}.qsdd", idx)),
        )?;
        let mut dict_header_buf: [u8; BYTES_DICTIONARY_HEADER] = [0; BYTES_DICTIONARY_HEADER];
        dict_file.seek(SeekFrom::Start(0))?;
        dict_file.read_exact(&mut dict_header_buf)?;
        let dict_header: DictionaryHeader = deserialize(&dict_header_buf).unwrap();
        Ok(dict_header)
    }

    pub fn inner_write_dict_header(
        idx: u64,
        source_db_root: PathBuf,
        dict_header: DictionaryHeader,
        mode: Mode,
    ) -> io::Result<()> {
        let mut dict_file = match mode {
            Mode::Modification => OpenOptions::new().write(true).open(
                source_db_root
                    .join(DICTIONARY)
                    .join(format!("{}.qsdd", idx)),
            )?,
            _ => File::create(
                source_db_root
                    .join(DICTIONARY)
                    .join(format!("{}.qsdd", idx)),
            )?,
        };
        dict_file.seek(SeekFrom::Start(0))?;
        serialize_into(&mut dict_file, &dict_header).unwrap();
        dict_file.sync_all()?;
        Ok(())
    }

    pub fn dict_get(&self, idx: u64, i: u64) -> io::Result<DictionaryBlock> {
        let mut dict_file = File::open(
            self.source_db_root
                .join(DICTIONARY)
                .join(format!("{}.qsdd", idx)),
        )?;
        dict_file.seek(SeekFrom::Start(
            (BYTES_DICTIONARY_HEADER as u64) + (BYTES_DICTIONARY_BLOCK as u64) * i,
        ))?;
        let mut dict_block_buf: [u8; BYTES_DICTIONARY_BLOCK] = [0; BYTES_DICTIONARY_BLOCK];
        dict_file.read_exact(&mut dict_block_buf)?;
        let dict_block: DictionaryBlock = deserialize(&dict_block_buf).unwrap();
        Ok(dict_block)
    }

    pub fn header(&self) -> Header {
        self.header
    }

    pub fn path(&self) -> PathBuf {
        self.source_db_root.clone()
    }

    /// It costs O(lgn)
    pub fn push(&mut self, source: &[u8], compress: bool) -> io::Result<()> {
        self.header.len += 1;
        Self::inner_write_header(self.source_db_root.clone(), self.header, Mode::Modification)?;
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.source_db_root.join(MASTER_DB))?;
        let metadata = db_file.metadata()?;
        self.push_dict(self.header.len, metadata.len())?;
        let block: Block = Block {
            nth: self.header.len,
            len: source.len() as u64,
        };
        db_file.seek(SeekFrom::Start(metadata.len()))?;
        serialize_into(&mut db_file, &block).ok();
        db_file.seek(SeekFrom::Start(metadata.len() + (BYTES_BLOCK as u64)))?;
        if compress {
            // Double encoding by arithmetic encoder and run-length encoder
            let mut encoder_rle = rle::Encoder::new(Vec::new());
            encoder_rle.write_all(source).unwrap();
            let (buf_rle, _): (Vec<u8>, _) = encoder_rle.finish();
            let mut encoder_ari = ari::ByteEncoder::new(BufWriter::new(Vec::new()));
            encoder_ari.write_all(&buf_rle).unwrap();
            let (buf_ari, _) = encoder_ari.finish();
            let inner = buf_ari.into_inner().unwrap();
            db_file.write_all(&inner)?;
        } else {
            db_file.write_all(source)?;
        }
        db_file.sync_all()?;
        Ok(())
    }

    pub fn push_dict(&self, idx: u64, offset: u64) -> io::Result<()> {
        // TODO: Reduce some overhead
        // - too many file open(s) occur
        //let dict_header = Self::inner_read_dict_header(0, self.source_db_root.clone())?;
        //dict_header.len += 1;
        let mut pivot: u64 = 1 << self.header.divisor_exp;
        let mut current: u64 = idx;
        let mut i = 0;
        loop {
            if current % pivot == 0 {
                let mut dict_header = Self::inner_read_dict_header(i, self.source_db_root.clone())?;
                dict_header.len += 1;
                Self::inner_write_dict_header(
                    i,
                    self.source_db_root.clone(),
                    dict_header,
                    Mode::Modification,
                )?;
                let mut dict_file = OpenOptions::new().write(true).open(
                    self.source_db_root
                        .clone()
                        .join(DICTIONARY)
                        .join(format!("{}.qsdd", i)),
                )?;
                let dict_block = DictionaryBlock {
                    nth: idx,
                    offset: offset,
                };
                dict_file.seek(SeekFrom::End(0))?;
                serialize_into(&mut dict_file, &dict_block).ok();
                break;
            }
            let current_block = self.dict_get(i, current / pivot)?;
            i = current_block.nth;
            current %= pivot;
            pivot >>= 1;
        }
        Self::inner_write_dict_header(
            idx,
            self.source_db_root.clone(),
            DictionaryHeader { len: 0 },
            Mode::Create,
        )?;
        Ok(())
    }
}
