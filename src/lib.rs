#![feature(fixed_size_array)]

extern crate bincode;
extern crate compress;
extern crate serde;

mod db;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        use crate::db::{DBFile, DEFAULT_HEADER};
        DBFile::new(["./test"].iter().collect(), None).unwrap();
        let mut f = DBFile::open(["./test"].iter().collect()).unwrap();
        assert_eq!(f.header(), DEFAULT_HEADER);
        f.push("An efficient database for storing code(s)".as_bytes(), true)
            .ok();
        f.push("Enumerative combinatorics".as_bytes(), true).ok();
        f.push("Algebra".as_bytes(), true).ok();
        f.push("Discrete mathematics".as_bytes(), true).ok();
        assert_ne!(DBFile::inner_read_header(f.path()).unwrap(), DEFAULT_HEADER);
    }
}
