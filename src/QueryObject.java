import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by YASH on 5/5/16.
 */
public class QueryObject implements Serializable {
    String fileName;
    ArrayList<String> columns;
    Integer IdVal;
    String serachByColumnName;
    String valueOfColumnName;
    Integer greaterThan;
    Integer lessThan;
}
