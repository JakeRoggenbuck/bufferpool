use std::fs::File;
use std::io::prelude::*;

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
    PID: PageID,
    data: [i64; 512],
}

impl Page {
    fn write(&self) {
        let filename = format!("page_{}", self.PID);
        let file = File::create(filename);

        match file {
            Ok(mut fp) => {
                // This is the fastest way to do this
                // I do not know all of the conditions that are needed to make this not break
                // TODO: Prove that this works always
                let bytes: [u8; 4096] = unsafe { std::mem::transmute(self.data) };
                // TODO: Use this result
                fp.write_all(&bytes).expect("Should be able to write.");
            }
            Err(..) => {
                println!("Error: Cannot open database file.");
            }
        }
    }
}

struct Bufferpool {
    pages: BHashMap<PageID, Page>,
}

impl Bufferpool {
    fn read(&self, index: usize) {}

    fn insert(&mut self, index: usize, value: usize) {}
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
