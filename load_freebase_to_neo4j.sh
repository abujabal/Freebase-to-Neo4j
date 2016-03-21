#!/bin/bash

# Run it like this:
# load_freebase_to_neo4j.sh freebaseDumpPath neo4jPath [numberOfTriples]
if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Illegal number of parameters"
    echo "Usage load_freebase_to_neo4j.sh freebaseDumpPath neo4jPath [numberOfTriples]"
    exit 1
fi
freebase=$1
neo4j=$2

triples=-1
if [ $# -eq 3 ]; then
    triples=$3
fi

# create database, set memory size
java -Xmx150G -cp neo4j/target/neo4j-0.0.1-SNAPSHOT-jar-with-dependencies.jar de.mpii.d5.neo4j.Main $freebase $neo4j $triples
