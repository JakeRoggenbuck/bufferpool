# bufferpool

[![Rust](https://img.shields.io/badge/Rust-1A5D8A?style=for-the-badge&logo=rust&logoColor=white)](https://github.com/JakeRoggenbuck?tab=repositories&q=&type=&language=rust&sort=stargazers)
[![CI](https://img.shields.io/github/actions/workflow/status/jakeroggenbuck/bufferpool/rust.yml?branch=main&style=for-the-badge)](https://github.com/JakeRoggenbuck/bufferpool/actions)

Database bufferpool implementation written in Rust

### What is a bufferpool

A bufferpool is a part of a database that handles what data gets kept in memory and what gets taken out of memory. Of course, the kernel keeps some disk pages in cache as well, but the cache in the bufferpool is on top of that cache. What makes a bufferpool most interesting, is the replacement strategy, and the way that you access the right value from the cache.

### Goals

This bufferpool will at first be asynchronous, but I will later make it async to improve performance. Part of this will be to test how much faster a concurrent bufferpool will be.
