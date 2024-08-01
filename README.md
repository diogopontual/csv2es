# csv2es

This is an exploratory project designed to familiarize myself with Rust. The project aims to read data from CSV files and ingest them into an Elasticsearch cluster. By undertaking this initiative, I hope to gain a deeper understanding of Rust's capabilities, particularly in handling file I/O operations and integrating with Elasticsearch.

# Current State

It is indexing the content. Next steps includes:

- [ ] Use the file name as index name;
- [ ] Allow user to set up the ES url and security;
- [ ] Print stats (errors, rows count...);
- [ ] Parallel processing of multiple files;
- [ ] Handle _id or id field as document id;