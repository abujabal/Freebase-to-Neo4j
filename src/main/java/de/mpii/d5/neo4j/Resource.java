package main.java.de.mpii.d5.neo4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A resource could be a subject, predicate, or an object.
 * An object could be an entity or a literal value
 * @author ajabal
 *
 */
public class Resource {
  // non-string literal e.g., int, double and boolean
  private static final Pattern NON_STRING_LIT = Pattern.compile("\"([^\"]+)\"\\^\\^([^:]+):(\\w+)");
  private String value;
  private String id;
  private String prefix;

  public Resource(String value) {
    this.value = value;
  }
  
  public String getValue() {
    return value;
  }
  
  public void setValue(String value) {
    this.value = value;
  }
  
  /**
   * Splits value into prefix and id.
   */
  public void setValues() {
    if (this.getValue().contains(":")) {
      int endIndex = this.getValue().indexOf(':');
      this.prefix = this.getValue().substring(0, endIndex);
      this.id = this.getValue().substring(endIndex + 1, this.getValue().length());
    }
  }
  
  /**
   * Checks whether a resource is literal or not.
   * @return boolean
   */
  public boolean isLiteral() {
    if (this.getValue().startsWith("\"")) {
      return true;
    }
    return false;
  }
  
  /**
   * get value out of literal resource.
   * @return the value of literal resource
   */
  public String normalizeLiteral() {
    String val = "";
    String type = "";
    Matcher m = NON_STRING_LIT.matcher(this.getValue());
    if (m.matches()) {
      // int, double, boolean, or datetime, example: "true"^^xsd:boolean
      val = m.group(1);
      type = m.group(3);
      if (type.equals("datetime")) {
        val = normalizeDate(val);
      }
      
    } else {
      int atIndex = this.getValue().lastIndexOf('@');
      val = this.getValue().substring(1, atIndex - 1);
      type = "string";
    }
    
    val = val.replace("\\u", "\\\\u");
    return val + ":@:" + type;
  }
  
  public String getId() {
    return id;
  }
  
  public String getPrefix() {
    return prefix;
  }
  
  /**
   * Since Neo4j does not support date datatype. This method converts a date into a long number.
   * It is a workaround which would work when comparing dates.
   * @param val Unnormalized date e.g., 2015-10-02
   * @return Normalized date
   */
  private String normalizeDate(String val) {
    String newVal = "";
    if (val.startsWith("T")) {
      newVal = val.substring(1, val.length()).replace(":", "").replace("Z", ""); 
    } else {
      if (val.contains("T")) {
        val = val.split("T")[0];
      }
      if (val.contains("-")) {
        String[] parts = val.split("-");
        for (String part : parts) {
          newVal += part;   
        }
      } else {
        newVal = val;
      }  
    }
    return newVal.trim();
  }
}
