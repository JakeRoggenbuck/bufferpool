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
