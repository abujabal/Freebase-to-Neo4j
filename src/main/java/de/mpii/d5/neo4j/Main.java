package main.java.de.mpii.d5.neo4j;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.HashMap;
import java.util.Map;

public class Main {
  /**
   * main method
   * @param args arg1 is the path to Freebase dump, arg2 is the path to store noe4j DB,
   *     arg3 is the number of triples to read (optional)
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: de.mpii.d5.neo4j.Main freebasePath "
          + "noe4jDatabaseDir [#triples]");
    }

    String freebasePath = args[0];
    String databaseDir = args[1];
    int numberOfTriples = -1;
    if (args.length == 3) {
      numberOfTriples = Integer.parseInt(args[2]);
    }
   
    BatchInserter db = null;
    try {
      // set these configuration based on the size of your data
      Map<String, String> config = new HashMap<String, String>();
      config.put("dbms.pagecache.memory", "70G");
      config.put("cache_type", "none");
      config.put("use_memory_mapped_buffers", "true");
      config.put("neostore.nodestore.db.mapped_memory", "10g");
      config.put("neostore.relationshipstore.db.mapped_memory", "10g");
      config.put("neostore.propertystore.db.mapped_memory", "5g");
      config.put("neostore.propertystore.db.strings.mapped_memory", "1g");
      config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
      config.put("neostore.propertystore.db.index.keys.mapped_memory", "1g");
      config.put("neostore.propertystore.db.index.mapped_memory", "10g");
      db = BatchInserters.inserter(databaseDir, config);
      Neo4jBatchHandler handler = new Neo4jBatchHandler(db);
      handler.createNeo4jDb(freebasePath, numberOfTriples);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      db.shutdown();
    }
  }
}
