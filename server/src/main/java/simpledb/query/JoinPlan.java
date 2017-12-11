package simpledb.index.query;


import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import java.util.*;

public class JoinPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2;
   private Schema sch = new Schema();
   

   public JoinPlan(Plan p1, Plan p2)) {
      this.p1 = p1;
      this.p2 = p2;
      
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }
   
   public Scan open() {
      Scan s1 = p1.open();
      SortScan s2 = (Scan) p2.open();
      return new JoinScan(s1, s2, fldname1, fldname2);
   }

   public int blocksAccessed() {
      return p1.blocksAccessed() + p2.blocksAccessed();
   }
   

   public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
                             p2.distinctValues(fldname2));
      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
   }
   

   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   public Schema schema() {
      return sch;
   }
}
