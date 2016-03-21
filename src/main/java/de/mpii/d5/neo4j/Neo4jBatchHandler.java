package de.mpii.d5.neo4j;

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

//consider adding types as labels, watch out . in types you might need to use ``

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
   * create noe4j database
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
          val = objectResource.handleLiteral();
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
   * store literal value as property of the subject node
   * @param subId subject id
   * @param r resource object
   * @param val literal value
   */
  private void insertLiteralValueAsProp(Long subId, Resource r, String val) {
    Map<String, Object> nodeProps = propsMap.get(subId);
    if (nodeProps == null) {
      nodeProps = new HashMap<String, Object>();
      propsMap.put(subId, nodeProps);
    }
    nodeProps.put(r.getId(), val);
    db.setNodeProperties(subId, nodeProps);
  }

  /**
   * create neo4j node with label "Entity"
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
   * create neo4j relationship between two nodes
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
