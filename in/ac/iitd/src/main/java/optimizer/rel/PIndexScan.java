package optimizer.rel;

import javafx.util.Pair;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import manager.StorageManager;
import org.apache.calcite.sql.type.SqlTypeName;
import storage.DB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {

    private final List<RexNode> projects;
    private final RelDataType rowType;
    private final RelOptTable table;
    private final RexNode filter;

    public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
        super(cluster, traitSet, table);
        this.table = table;
        this.rowType = deriveRowType();
        this.filter = filter;
        this.projects = projects;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PIndexScan(getCluster(), traitSet, table, filter, projects);
    }

    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "PIndexScan";
    }

    public String getTableName() {
        return table.getQualifiedName().get(1);
    }

    @Override
    public List<Object[]> evaluate(StorageManager storage_manager) {
        String tableName = getTableName();
        System.out.println("Evaluating PIndexScan for table: " + tableName);
        int fileId = storage_manager.return_file_id(tableName);
        if (!storage_manager.check_file_exists(tableName)) {
            System.out.println("Table not found: " + tableName);
            return Collections.emptyList();
        }

        String operator;
        Object value,columnPos;
        if (filter instanceof RexCall) {
            RexCall call = (RexCall) filter;
            operator = call.getOperator().getName(); // Get operator name
            RexNode leftOperand = call.getOperands().get(0);
            RexNode rightOperand = call.getOperands().get(1);
            columnPos = extractValue(leftOperand); // Extract column reference
            value = extractValue(rightOperand); // Extract integer value

        }else{
            throw new IllegalArgumentException("Invalid filter type");
        }

        List<Object[]> result = new ArrayList<>();
        DB database = storage_manager.getDb();
        int num_records = database.get_num_records(fileId);
        int j=1;
        for (int i = 0; i < num_records; ) {
            List<Object[]> temp = storage_manager.get_records_from_block(tableName,j);
            j++;
            i+=temp.size();
            for (int k = 0; k < temp.size(); k++) {
                Object[] row = temp.get(k);
                Object req = row[(int)columnPos];
                if(req!=null){
                    if(operator.equals("=")){
                        if(compareKeys(req,value)==0){
                            result.add(row);
                        }
                    }else if(operator.equals(">")){
                        if(compareKeys(req,value)>0){
                            result.add(row);
                        }
                    }else if(operator.equals(">=")){
                        if(compareKeys(req,value)>=0){
                            result.add(row);
                        }
                    }else if(operator.equals("<")){
                        if(compareKeys(req,value)<0){
                            result.add(row);
                        }
                    }else{
                        if(compareKeys(req,value)<=0){
                            result.add(row);
                        }
                    }
                }
            }
        }

        return result;
    }

    private Object extractValue(RexNode node) {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            RelDataType typeName = literal.getType();
            switch (typeName.getSqlTypeName()) {
                case VARCHAR:
                    return literal.getValueAs(String.class);
                case INTEGER:
                    return literal.getValueAs(Integer.class);
                case DECIMAL:
                    if(typeName.getPrecision() == 0) {
                        return literal.getValueAs(Integer.class);
                    }else{
                        return literal.getValueAs(Double.class);
                    }
                case BOOLEAN:
                    return literal.getValueAs(Boolean.class);
                case FLOAT:
                    return literal.getValueAs(Float.class);
                case DOUBLE:
                    return literal.getValueAs(Double.class);
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + typeName.getSqlTypeName());
            }
        }else if(node instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) node;
            return Integer.valueOf(inputRef.getIndex());
        }else{
            throw new IllegalArgumentException("Invalid operand type");
        }

    }

    int compareKeys(Object k1, Object k2) {
        if (k1 instanceof String && k2 instanceof String) {
            return ((String) k1).compareTo((String) k2);
        } else if (k1 instanceof Integer && k2 instanceof Integer) {
            return Integer.compare((Integer) k1, (Integer) k2);
        } else if (k1 instanceof Boolean && k2 instanceof Boolean) {
            return Boolean.compare((Boolean) k1, (Boolean) k2);
        } else if (k1 instanceof Float && k2 instanceof Float) {
            return Float.compare((Float) k1, (Float) k2);
        } else if (k1 instanceof Double && k2 instanceof Double) {
            return Double.compare((Double) k1, (Double) k2);
        } else {
            throw new IllegalArgumentException("Unsupported data types for comparison: " + k1.getClass().getSimpleName() + " and " + k2.getClass().getSimpleName());
        }
    }


}