# egraph

egraph is designed to process and load eCAR data into an active Dgraph database

Warning: egraph contains command line execution that takes user-controlled strings as input. The fork "egraph-lite" should be used in combination with Dgraph's live loader (also allows specifying multiple files) to avoid this.

### Running egraph
After building the go binary for egraph, egraph can be called as an exectuable with the flags
```
-watch : Would you like to watch a directory (default: false)
``` 
```
-dir : Directory to be monitored for new .json/.json.gz eCAR files (default: Current working directory)
```
```
-write : Specify an eCAR file (.json or .json.gz) that you would like to rewrite into RDF triples (default: None)
```
```
-only : For a given predicate and value, egraph will only include eCAR events including a valid match (does not extend to "properties" values)
"-only=predicate:value", "-only=actorID:c8a8a4e1-b5b5-48c0-92bb-6a41f5b1b9c8"
```
```
-load : Inital file to load (default: None)
``` 
```
-url : Address of dGraph for gRPC connection (default: "localhost:9080")
```
```
-zero : Address of dGraph Zero for live loader (default: "localhost:6080") // If you are running Dgraph standalone and cannot connect to zero try -zero=localhost:5080
```
```
-drop : Drop all data during setup (default: false) 
```
```
-setup : Add eCAR schema to Dgraph (Does not include properties currently, lines 37-48) (default: false) 
```
It is also reccomended when loading large files to use caffeinate to keep the disk from sleeping which can disrupt live loading, however rdf files over ~30GB seem to be unable to finish regardless (I expect this is on Dgraphs end).
Example:
```
caffeinate -s -u egraph -load=example_eCAR.json.gz -drop=true 
```

## Stats
The current build of egraph now includes an updated schema which adds a number of new predicates. However,
this seems to have slowed down write times significantly as well as loading the rdf N-Quads through the live loader.

For 1 GB of eCAR in gzipped-json:
~300M+ RDF triples (current build writes actorID triples unecessarily, to be addressed)
~37k unique Actor IDs
~10 minutes to rewrite into RDF triples (Core i9)
~3.5 hours to load RDF triples through the live loader
