use std::fs::File;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

type BHashMap<K, V> = std::collections::HashMap<K, V>;
// type BHashMap<K, V> = FxHashMap<K, V>;
// type BHashMap<K, V> = ShardMap<K, V>;
// type BHashMap<K, V> = BTreeMap<K, V>;

// Idea for ID values:
// When you insert into a page, if's full, you then allocate a new page
// Each time you insert, this record gets a Record ID, and the value corresponds to when it was
// inserted. This tells us the location in memory. Using this, we can tell both which page it
// should be in, and where in the page it should be located
//
// If I want ID 0, I go to the 0th page, and the 0th value
// If I want ID 100, I go to the 0th page, and the 100th value
// If I want ID 513, I go to the 1st page, and the 0th value
//
// This way, when we check for a value, we might see that that page is not actually loaded into
// memory, this will cause us to find the file and then load it into the bufferpool.
//
// The page is `index // 512` and the value index is `index % 512`

type PageID = usize;

struct Page {
    pid: PageID,
    data: Option<[i64; 512]>,
}

impl Page {
    fn open(&mut self) {
        let data = self.read();

        self.data = Some(data);
    }

    fn new(pid: PageID) -> Self {
        Page { pid, data: None }
    }

    fn get_page_path(&self) -> String {
        format!("page_{}", self.pid)
    }

    fn write_page(&self) {
        let filename = self.get_page_path();
        let file = File::create(filename);

        match file {
            Ok(mut fp) => {
                // This is the fastest way to do this
                // I do not know all of the conditions that are needed to make this not break
                // TODO: Prove that this works always
                if let Some(d) = self.data {
                    let bytes: [u8; 4096] = unsafe { std::mem::transmute(d) };
                    // TODO: Use this result
                    fp.write_all(&bytes).expect("Should be able to write.");
                }
            }
            Err(..) => {
                println!("Error: Cannot open database file.");
            }
        }
    }

    fn read_page(&self) -> [i64; 512] {
        let filename = self.get_page_path();
        let mut file = File::open(filename).expect("Should open file.");
        let mut buf: [u8; 4096] = [0; 4096];

        let _ = file.read(&mut buf[..]).expect("Should read.");

        // TODO: Make sure this works as expected always
        let values: [i64; 512] = unsafe { std::mem::transmute(buf) };
        return values;
    }

    fn write_value(&mut self, index: usize, value: i64) {
        if let Some(mut d) = self.data {
            d[index] = value;
        }
    }

    fn read_value(&self, index: usize) -> Option<i64> {
        if let Some(d) = self.data {
            return Some(d[index]);
        }

        None
    }
}

struct Bufferpool {
    pages: BHashMap<PageID, Arc<Mutex<Page>>>,
}

impl Bufferpool {
    fn read(&self, index: usize) {}

    fn insert(&mut self, index: usize, value: i64) {
        let pid: usize = index / 512;
        let index_in_page = index % 512;

        let mut page: Option<Arc<Mutex<Page>>> = None;

        if self.pages.contains_key(&index_in_page) {
            // Open the page cause it was not opened
            let mut new_page = Page::new(pid);
            new_page.open();

            // Make an Arc
            page = Some(Arc::new(Mutex::new(new_page)));
            self.pages.insert(pid, page.clone().unwrap());
        } else {
            // Get the page because it was opened
            let poption = self.pages.get(&pid);
            if let Some(p) = poption {
                page = Some(p.clone());
            }
        }

        if let Some(p) = page {
            let mut b = p.lock().unwrap();
            b.write_value(index_in_page, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_integration_test() {
        let mut bpool = Bufferpool::new();

        // Each page is 4KB on an x86-64 machine
        // Adding 4 pages, is 16KB or 16384 bytes
        bpool.set_page_limit(4);
        assert_eq!(bpool.page_limit, 4);

        // Create 4KB of data
        let four_k_of_data: [i64; 512] = [0; 512];
        assert_eq!(std::mem::size_of_val(&four_k_of_data), 512 * 8);

        // Create a page of data
        let page_1 = Page::new(four_k_of_data);
        assert_eq!(page_1.size(), 4096);

        // Check that the bpool is empty
        assert_eq!(bpool.size(), 0);
        assert!(bpool.empty());

        // Insert the page of data
        bpool.insert(page_1);
        assert_eq!(bpool.size(), 1);
        assert!(!bpool.empty());

        assert!(!bpool.full());

        // Insert 3 more pages to fill the bufferpool
        let page_2 = Page::new(four_k_of_data);
        let page_3 = Page::new(four_k_of_data);
        let page_4 = Page::new(four_k_of_data);

        bpool.insert(page_2);
        bpool.insert(page_3);
        bpool.insert(page_4);

        assert_eq!(bpool.size(), 4);
        assert!(bpool.full());

        // Add another page after it's full
        let page_5 = Page::new(four_k_of_data);

        bpool.insert(page_5);

        // Since the limit is 4, it should have removed one page to allow space for this new one
        assert_eq!(bpool.size(), 4);
        assert!(bpool.full());

        // Read the 0th page
        let read_page = bpool.read(0);

        // Read the first value
        assert_eq!(read_page.read(0), 0);
    }
}
