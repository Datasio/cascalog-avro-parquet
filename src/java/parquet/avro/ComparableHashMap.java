package parquet.avro;

import java.util.HashMap;

public class ComparableHashMap<K,V> extends HashMap<K,V> implements Comparable<ComparableHashMap> {

  public ComparableHashMap() {
    super();
  }
  public int compareTo(ComparableHashMap m2) {
    for (K key : this.keySet()) {
      if (((Comparable) this.get(key)).compareTo(m2.get(key)) != 0) {
        return -1;
      }
    }
    return 0;
  }
}


