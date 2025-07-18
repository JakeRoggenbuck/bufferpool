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

#[derive(Debug)]
struct Page {
    pid: PageID,
    data: Option<[i64; 512]>,
    index: usize,
}

impl Page {
    fn open(&mut self) {
        let data = self.read_page();

        self.data = Some(data);
    }

    fn new(pid: PageID) -> Self {
        Page {
            pid,
            data: None,
            index: 0,
        }
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

    fn set_value(&mut self, index: usize, value: i64) {
        if let Some(mut d) = self.data {
            d[index] = value;
        }

        self.index += 1;
    }

    fn set_all_values(&mut self, input: [i64; 512]) {
        self.data = Some(input);
    }

    fn get_value(&self, index: usize) -> Option<i64> {
        if let Some(d) = self.data {
            return Some(d[index]);
        }

        None
    }

    fn size(&self) -> usize {
        self.index
    }

    fn capacity(&self) -> usize {
        4096
    }
}

struct Bufferpool {
    // Right now, there is no removal strategy
    pages: BHashMap<PageID, Arc<Mutex<Page>>>,
    page_index: PageID,
    page_limit: usize,
}

impl Bufferpool {
    fn new() -> Self {
        Bufferpool {
            pages: BHashMap::new(),
            page_index: 0,
            page_limit: 0,
        }
    }

    fn set_page_limit(&mut self, limit: usize) {
        self.page_limit = limit;
    }

    fn create_page(&mut self) -> Arc<Mutex<Page>> {
        let p = Page::new(self.page_index);
        let page = Arc::new(Mutex::new(p));

        self.pages.insert(self.page_index, page.clone());
        self.page_index += 1;
        return page.clone();
    }

    fn size(&self) -> usize {
        self.page_index
    }

    fn empty(&self) -> bool {
        self.size() == 0
    }

    fn full(&self) -> bool {
        self.page_index >= self.page_limit
    }

    fn fetch(&self, index: usize) -> Option<i64> {
        let pid: usize = index / 512;
        let index_in_page = index % 512;

        if self.pages.contains_key(&pid) {
            let page = self.pages.get(&pid);

            if let Some(p) = page {
                let b = p.lock().unwrap();
                return b.get_value(index_in_page);
            }
        }

        None
    }

    fn insert(&mut self, index: usize, value: i64) {
        let pid: usize = index / 512;
        let index_in_page = index % 512;

        if self.pages.contains_key(&pid) {
            // Get the page because it was opened
            let poption = self.pages.get(&pid);

            let mut b = poption.unwrap().lock().unwrap();
            b.set_value(index_in_page, value);

            return;
        }

        // Open the page cause it was not opened
        let mut new_page = Page::new(pid);
        new_page.open();

        // Make an Arc
        let page = Some(Arc::new(Mutex::new(new_page)));
        self.pages.insert(pid, page.clone().unwrap());
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

        assert_eq!(bpool.size(), 0);
        assert!(bpool.empty());

        // Create a page of data
        let page_1_arc = bpool.create_page();
        {
            let mut page_1 = page_1_arc.lock().unwrap();
            page_1.set_all_values(four_k_of_data);
            page_1.write_page();

            assert_eq!(page_1.size(), 0);
            assert_eq!(page_1.capacity(), 4096);
        }

        assert_eq!(bpool.size(), 1);
        assert!(!bpool.empty());
        assert!(!bpool.full());

        // Insert 3 more pages to fill the bufferpool
        let page_2_arc = bpool.create_page();
        {
            let mut page_2 = page_2_arc.lock().unwrap();
            page_2.set_all_values(four_k_of_data);
            page_2.write_page();
        }

        let page_3_arc = bpool.create_page();
        {
            let mut page_3 = page_3_arc.lock().unwrap();
            page_3.set_all_values(four_k_of_data);
            page_3.write_page();
        }

        let page_4_arc = bpool.create_page();
        {
            let mut page_4 = page_4_arc.lock().unwrap();
            page_4.set_all_values(four_k_of_data);
            page_4.write_page();
        }

        assert_eq!(bpool.size(), 4);
        assert!(bpool.full());

        // Add another page after it's full
        let page_5_arc = bpool.create_page();
        {
            let mut page_5 = page_5_arc.lock().unwrap();
            page_5.set_all_values(four_k_of_data);
            page_5.write_page();
        }

        // Since the limit is 4, it should have removed one page to allow space for this new one
        // TODO: Make a removal strategy and this will be true
        //assert_eq!(bpool.size(), 4);
        assert!(bpool.full());

        bpool.insert(0, 100);

        // Read the 0th value
        //let val = bpool.fetch(0);

        // Read the first value
        //assert_eq!(val, Some(100));
    }
}
