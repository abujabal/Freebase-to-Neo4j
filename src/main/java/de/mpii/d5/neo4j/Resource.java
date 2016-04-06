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
  // string literal
  private static final Pattern STRING_LIT = Pattern.compile("\"([^\"]+)\"@(\\w+)");
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
   * split value into prefix and id
   */
  public void setValues() {
    if (this.getValue().contains(":")) {
      int endIndex = this.getValue().indexOf(':');
      this.prefix = this.getValue().substring(0, endIndex);
      this.id = this.getValue().substring(endIndex + 1, this.getValue().length());
    }
  }
  /**
   * check whether a resource is literal
   * @return boolean
   */
  public boolean isLiteral() {
    if (this.getValue().startsWith("\"")) {
      return true;
    }
    return false;
  }
  /**
   * get value out of literal resource
   * @return the value of literal resource
   */
  public String handleLiteral() {
    String val = "";
    Matcher m = NON_STRING_LIT.matcher(this.getValue());
    Matcher m2 = STRING_LIT.matcher(this.getValue());
    if (m.matches()) {
      // int, double, boolean, or datetime, example: "true"^^xsd:boolean
      val = m.group(1);
    } else if (m2.matches()) {
      // string, example: "abdalghani"@en
      val = m2.group(1);
    }
    // insert escape chars
    val = val.replace("\\u", "\\\\u");
    return val;
  }
  
  public String getId() {
    return id;
  }
  
  public String getPrefix() {
    return prefix;
  }
}
