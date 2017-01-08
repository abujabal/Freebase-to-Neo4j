package main.java.de.mpii.d5.neo4j;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import java.util.concurrent.TimeUnit;

public class SchemaIndexOnProp {

  /**
   * Creates index over a particular property of a node (__MID__ in our case). To able to do this, 
   * a node must have a label e.g., Entity in our case
   * @param path
   *            neo4j database path
   */
  public static void create(String path) {
    GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path);
    {
      // START SNIPPET: createIndex
      IndexDefinition indexDefinition;
      try (Transaction tx = graphDb.beginTx()) {
        Schema schema = graphDb.schema();
        indexDefinition = schema.indexFor(DynamicLabel.label("Entity"))
            .on( "__MID__" ).create();
        tx.success();
      }
      // END SNIPPET: createIndex
      // START SNIPPET: wait
      try (Transaction tx = graphDb.beginTx()) {
        Schema schema = graphDb.schema();
        schema.awaitIndexOnline(indexDefinition, 2, TimeUnit.DAYS);
      }
      // END SNIPPET: wait
    }
  }
  
  /**
   * main method.
   * @param args arg1 is the path to neo4j database
   */
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err
      .println("Usage: main.java.de.mpii.d5.neo4j.SchemaIndexOnProp neo4jDatabaseDir");
    }
    String databaseDir = args[0];
    SchemaIndexOnProp.create(databaseDir);
  }
}
