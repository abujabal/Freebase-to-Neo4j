package main.java.de.mpii.d5.neo4j;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Load Freebase triples into a neo4j graph database. 
 * Each noe4j node has a label called Entity to enable us to create schema 
 * indexes over node properties e.g., __MID__.
 * Each node has two properties: __MID__, __PREFIX__.
 * Literal objects are stored as properties of the subject
 * 
 * @author Abdalghani Abujabal - abujabal@mpi-inf.mpg.de
 * 
 *  @version 1.0
 */


public class Neo4jBatchHandler {
  private int totalTriples = 0;
  private int addedNodes = 0;
  private int addedRelationships = 0;
  private BatchInserter db;
  // property for each neo4j node which stores resource's id
  private static final String MID_PROPERTY = "__MID__";
  // property for each noe4j node which stores preceding prefix of a resource
  private static final String PREFIX_PROPERTY = "__PREFIX__";
  // <resource id, node id>: to keep track of inserted nodes
  private Map<String, Long> tmpIndex = new HashMap<String, Long>();
  // <node id, labels>: to keep track of node's labels
  private Map<Long, HashSet<Label>> labelsMap = new HashMap<Long, HashSet<Label>>();
  // <node id, properties>: to keep track of node's properties
  private Map<Long, Map<String, Object>> propsMap = new HashMap<Long, Map<String, Object>>();
  
  public Neo4jBatchHandler(BatchInserter db) {
    this.db = db;
  }

  /**
   * Creates noe4j database.
   * @param path
   *            Freebase triples path
   * @param numberOfTriples number of Freebase triples to read
   */
  public void createNeo4jDb(String path, int numberOfTriples) {
    String line = "";
    try {
      int count = 0;
      int millions = 0;
      // Specifying encoding is important to store data properly
      BufferedReader br = new BufferedReader(new InputStreamReader(
          new FileInputStream(path), "UTF-8"));

      line = br.readLine();
      line = br.readLine();
      while (line != null) {
        count++;
        boolean literalObject = false;
        String[] fields = line.split("\t");
        // subject resource
        String subjectStr = fields[0];
        Resource subjectResource = new Resource(subjectStr);
        subjectResource.setValues();
        
        // object resource
        String objectStr = fields[2].trim();
        // at the end of each object there is dot
        objectStr = objectStr.substring(0, objectStr.length() - 1);
        
        Resource objectResource = new Resource(objectStr);
        String val = "";
        if (!objectResource.isLiteral()) {
          // entity, example: fb:m.0n7x41f
          objectResource.setValues();
        } else {
          // literal
          literalObject = true;
          val = objectResource.normalizeLiteral();
        }
        // predicate resource
        String predicateStr = fields[1];
        Resource predicateResource = new Resource(predicateStr);
        predicateResource.setValues();
        
        // Check subject node existence
        Long subjectNodeId = tmpIndex.get(subjectResource.getId());
        if (subjectNodeId == null) {
          subjectNodeId = this.createNeo4jNode(subjectResource);
        }
        if (literalObject) {
          this.insertLiteralValueAsProp(subjectNodeId, predicateResource, val);
        } else {
          // Check object node existence
          Long objectNodeId = tmpIndex.get(objectResource.getId());
          if (objectNodeId == null) {
            objectNodeId = this.createNeo4jNode(objectResource);
          }
          // create relation between subject and object nodes
          createNeo4jRelation(predicateResource, subjectNodeId, objectNodeId);
        }
        totalTriples++;
        if (totalTriples == numberOfTriples) {
          break;
        }
        // print every 10M triples
        if (count == 10000000) {
          millions += 10;
          System.out.println("triples = " + millions + "M");
          count = 0;
        }
       
        line = br.readLine();
      }
      br.close();
      System.out.println("totalTriples = " + totalTriples + " : " 
          + "addedRelationships = " + addedRelationships + " : " 
          + "addedNodes = " + addedNodes);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** 
   * Stores literal value as property of the subject node. If the property has multiple values,
   * it will be stored as an array.
   * @param subId subject id
   * @param r resource predicate
   * @param val literal value
   */
  private void insertLiteralValueAsProp(Long subId, Resource r, String val) {
    String value = val.split(":@:")[0];
    String type = val.split(":@:")[1];
    boolean boolVal = false;
    long longVal = 0;
    double doubleVal = 0.0;
    Object objVal = null;
    if (type.equals("boolean")) {
      boolVal = Boolean.parseBoolean(value);
      objVal = boolVal;
    } else if (type.equals("double")) {
      doubleVal = Double.parseDouble(value);
      objVal = doubleVal;
    } else if (type.equals("int") || type.equals("datetime")) {
      longVal = Long.parseLong(value);
      objVal = longVal;
    } else {
      objVal = value;
    }
    Map<String, Object> nodeProps = propsMap.get(subId);
    if (nodeProps == null) {
      nodeProps = new HashMap<String, Object>();
      propsMap.put(subId, nodeProps);
    }
    // create array of values for the property if needed
    Object existingValues = nodeProps.get(r.getId());
    Object dataToInsert = null;
    if (existingValues == null) {
      // thus far, this property has only one value. No array is needed
      nodeProps.put(r.getId(), objVal);
    } else {
       // this property has more than one value. Array is needed.
      if (type.equals("boolean")) {
          // check whether existingValues is a primitive type or an array
        if (existingValues instanceof Boolean) {
          boolean[] boolValues = new boolean[2];
          boolValues[0] = (Boolean)existingValues;
          boolValues[1] = (Boolean)objVal;
          dataToInsert = boolValues;
        } else {
          // array of boolean
          boolean[] boolExisting = (boolean[])existingValues;
          boolean[] boolValues = new boolean[boolExisting.length + 1];
          for (int i = 0; i < boolExisting.length; i++) {
            boolValues[i] = boolExisting[i];
          }
          boolValues[boolValues.length - 1] = (Boolean)objVal;
          dataToInsert = boolValues;
        }
      } else if (type.equals("double")) {
        // check whether existingValues is a primitive type or an array
        if (existingValues instanceof Double) {
          double[] doubleValues = new double[2];
          doubleValues[0] = (Double)existingValues;
          doubleValues[1] = (Double)objVal;
          dataToInsert = doubleValues;
        } else {
          // array of boolean
          double[] doubleExisting = (double[])existingValues;
          double[] doubleValues = new double[doubleExisting.length + 1];
          for (int i = 0; i < doubleExisting.length; i++) {
            doubleValues[i] = doubleExisting[i];
          }
          doubleValues[doubleValues.length - 1] = (Double)objVal;
          dataToInsert = doubleValues;
        } 
      } else if (type.equals("int") || type.equals("datetime")) {
         // check whether existingValues is a primitive type or an array
        if (existingValues instanceof Long) {
          long[] longValues = new long[2];
          longValues[0] = (Long)existingValues;
          longValues[1] = (Long)objVal;
          dataToInsert = longValues;
        } else {
          // array of boolean
          long[] longExisting = (long[])existingValues;
          long[] longValues = new long[longExisting.length + 1];
          for (int i = 0; i < longExisting.length; i++) {
            longValues[i] = longExisting[i];
          }
          longValues[longValues.length - 1] = (Long)objVal;
          dataToInsert = longValues;
        } 
      } else {
        // check whether existingValues is a primitive type or an array
        if (existingValues instanceof String) {
          String[] stringValues = new String[2];
          stringValues[0] = (String)existingValues;
          stringValues[1] = (String)objVal;
          dataToInsert = stringValues;
        } else {
          // array of boolean
          String[] stringExisting = (String[])existingValues;
          String[] stringValues = new String[stringExisting.length + 1];
          for (int i = 0; i < stringExisting.length; i++) {
            stringValues[i] = stringExisting[i];
          }
          stringValues[stringValues.length - 1] = (String)objVal;
          dataToInsert = stringValues;
        } 
      }
      nodeProps.put(r.getId(), dataToInsert);
    }
    db.setNodeProperties(subId, nodeProps);
  }

  /**
   * Creates neo4j node with label "Entity".
   * @param resource either subject or object
   * @return nodeId an automatically generated id once a node is stored in noe4j DB
   * 
   */
  private Long createNeo4jNode(Resource resource) {
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(MID_PROPERTY, resource.getId());
    props.put(PREFIX_PROPERTY, resource.getPrefix());
    Long nodeId = db.createNode(props);
    propsMap.put(nodeId, props);
    tmpIndex.put(resource.getId(), nodeId);
    addedNodes++;
    Label label = DynamicLabel.label("Entity");
    HashSet<Label> labels = labelsMap.get(nodeId);
    if (labels == null) {
      labels = new HashSet<Label>();
      labelsMap.put(nodeId, labels);
    }
    labels.add(label);
    db.setNodeLabels(nodeId, labels.toArray(new Label[labels.size()]));
    return nodeId;
  }
  
  /**
   * Creates neo4j relationship between two nodes.
   * @param predicateResource predicate
   * @param subjectNodeId subject node id
   * @param objectNodeId object node id
   */
  private void createNeo4jRelation(Resource predicateResource, long subjectNodeId, 
      long objectNodeId) {
    RelationshipType relType = DynamicRelationshipType.withName(predicateResource.getId());
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(PREFIX_PROPERTY, predicateResource.getPrefix());
    db.createRelationship(subjectNodeId, objectNodeId, relType, props);
    addedRelationships++;
  }
}
