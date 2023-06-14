//! Tar archive reader with slightly relaxed semantics (Send, LendingIterator)

use std::io;
use std::io::prelude::*;

use crate::other;
use crate::Header;

/// Simple tar entries reader
pub struct SimpleEntries<R> {
    obj: R,
    ignore_zeros: bool,
    padding: usize,
}

impl<R: Read> SimpleEntries<R> {
    /// SimpleEntries constructor
    pub fn new(obj: R) -> Self {
        Self {
            obj,
            ignore_zeros: false,
            padding: 0,
        }
    }
}

/// A read-only view into an entry of an archive.
pub struct SimpleEntry<'a, R: Read> {
    /// `Read` instance with the Entry contents
    pub obj: std::io::Take<&'a mut R>,
    /// Entry metadata
    pub header: Header,
    /// Entry size
    pub size: u64,
}

/// Iterator LendingIterator which permit to iterate over items but where the data may be stored within the iterator itself
pub trait LendingIterator {
    /// The type of the elements being iterated over
    type Item<'a>
    where
        Self: 'a;

    /// Advances the iterator and returns the next value
    fn next(&mut self) -> Option<Self::Item<'_>>;
}

impl<R: Read> LendingIterator for SimpleEntries<R> {
    type Item<'a> = io::Result<SimpleEntry<'a, R>> where R: 'a, Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.next_entry_raw().transpose()
    }
}

impl<'a, R: Read> Drop for SimpleEntry<'a, R> {
    fn drop(&mut self) {
        // exhaust reader so the reader position is at the next entry in tar
        let mut buf = [0u8; 4096 * 8];
        while let Ok(read) = self.obj.read(&mut buf) {
            if read == 0 {
                break;
            }
        }
    }
}

impl<R: Read> SimpleEntries<R> {
    fn next_entry_raw(&mut self) -> io::Result<Option<SimpleEntry<'_, R>>> {
        // skip over padding
        if !try_read_all(&mut self.obj, &mut [0; 512][..self.padding])? {
            return Ok(None);
        }

        let mut header = Header::new_old();
        loop {
            // EOF is an indicator that we are at the end of the archive.
            if !try_read_all(&mut self.obj, header.as_mut_bytes())? {
                return Ok(None);
            }

            // If a header is not all zeros, we have another valid header.
            // Otherwise, check if we are ignoring zeros and continue, or break as if this is the
            // end of the archive.
            if !header.as_bytes().iter().all(|i| *i == 0) {
                break;
            }

            if !self.ignore_zeros {
                return Ok(None);
            }
        }

        // Make sure the checksum is ok
        let sum = header.as_bytes()[..148]
            .iter()
            .chain(&header.as_bytes()[156..])
            .fold(0, |a, b| a + (*b as u32))
            + 8 * 32;
        let cksum = header.cksum()?;
        if sum != cksum {
            return Err(other("archive header checksum mismatch"));
        }

        let size = header.entry_size()?;
        self.padding = (512 - size % 512) as usize;

        let next = SimpleEntry {
            obj: (&mut self.obj).take(size),
            header,
            size,
        };

        Ok(Some(next))
    }
}

/// Try to fill the buffer from the reader.
///
/// If the reader reaches its end before filling the buffer at all, returns `false`.
/// Otherwise returns `true`.
fn try_read_all<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        match r.read(&mut buf[read..])? {
            0 => {
                if read == 0 {
                    return Ok(false);
                }

                return Err(other("failed to read entire block"));
            }
            n => read += n,
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /* fails to compile - good!
    #[test]
    fn test_multiple_entries_not_allowed() {
        let mut entries = SimpleEntries::new(&[][..]);
        let e1 = entries.next();
        let e2 = entries.next();
        drop(e1);
        drop(e2);
    }
    */

    #[test]
    fn test_usage() {
        let input = std::fs::File::open("tests/archives/reading_files.tar").unwrap();
        let mut entries = SimpleEntries::new(input);

        {
            let mut a = entries.next().expect("expected entry a present").unwrap();
            assert_eq!(&*a.header.path_bytes(), b"a");
            let mut s = String::new();
            a.obj.read_to_string(&mut s).unwrap();
            assert_eq!(s, "a\na\na\na\na\na\na\na\na\na\na\n");
        }

        {
            let mut b = entries.next().expect("expected entry b present").unwrap();
            assert_eq!(&*b.header.path_bytes(), b"b");
            let mut s = String::new();
            b.obj.read_to_string(&mut s).unwrap();
            assert_eq!(s, "b\nb\nb\nb\nb\nb\nb\nb\nb\nb\nb\n");
        }

        assert!(entries.next().is_none());
    }

    #[test]
    fn test_send_sync_static() {
        fn require_send_sync_static<T: Send + Sync + 'static>(_: T) {}

        let input: &'static [u8] = &[][..];
        require_send_sync_static(SimpleEntries::new(input));
    }

    #[test]
    fn test_scoped_threads() {
        use std::thread;

        let input = std::fs::File::open("tests/archives/reading_files.tar").unwrap();
        let mut entries = SimpleEntries::new(input);

        // test 1: move the entry into a scoped thread
        {
            let mut a = entries.next().expect("expected entry a present").unwrap();

            thread::scope(|s| {
                s.spawn(|| {
                    assert_eq!(&*a.header.path_bytes(), b"a");
                    let mut s = String::new();
                    a.obj.read_to_string(&mut s).unwrap();
                    assert_eq!(s, "a\na\na\na\na\na\na\na\na\na\na\n");
                });
            });
        }

        // test 2: move the entries iterator into a scoped thread
        thread::scope(|s| {
            s.spawn(|| {
                let mut b = entries.next().expect("expected entry b present").unwrap();
                assert_eq!(&*b.header.path_bytes(), b"b");
                let mut s = String::new();
                b.obj.read_to_string(&mut s).unwrap();
                assert_eq!(s, "b\nb\nb\nb\nb\nb\nb\nb\nb\nb\nb\n");
            });
        });
    }
}
