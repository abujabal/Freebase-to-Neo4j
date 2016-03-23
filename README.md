
# Freebase to Neo4j Graph Database Loader

## Freebase dump used is based on: 
            https://github.com/percyliang/sempre

It is a canonicalized and scaled down dump

## Easy setup

1. Clone the GitHub repository:

            git clone https://github.com/abujabal/Freebase-to-Neo4j

2. Compile the source code (this produces `target/neo4j-0.0.1-SNAPSHOT-jar-with-dependencies.jar`):

            mvn package

3. Run code by invoking the Main class and passing the path to Freebase dump as well as a directory to store Neo4j db
            