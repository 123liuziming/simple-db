package simpledb;

import jdk.jshell.spi.ExecutionControl;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private Op what;

    private Map<Field, Integer> aMap;

    // 用于avg
    private Map<Field, Integer> cntMap;

    private Iterator<Map.Entry<Field, Integer>> iterator;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aMap = new HashMap<>();
        this.cntMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = gbfield == Aggregator.NO_GROUPING ? new StringField("no_grouping", 20) : tup.getField(gbfield);
        Integer val = aMap.get(key);
        IntField aField = (IntField) tup.getField(afield);
        switch (what) {
            case MIN:
                aMap.put(key, Math.min(val == null ? Integer.MAX_VALUE : val, aField.getValue()));
                break;
            case AVG:
                aMap.put(key, (val == null ? 0 : val) + aField.getValue());
                cntMap.put(key, cntMap.getOrDefault(key, 0) + 1);
                break;
            case SUM:
                aMap.put(key, (val == null ? 0 : val) + aField.getValue());
                break;
            case MAX:
                aMap.put(key, Math.max(val == null ? Integer.MIN_VALUE : val, aField.getValue()));
                break;
            case COUNT:
                aMap.put(key, (val == null ? 1 : val + 1));
                break;
            default:
                throw new UnsupportedOperationException("not implement yet");
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {

        return new Operator() {
            @Override
            protected Tuple fetchNext() throws DbException, TransactionAbortedException {
                try {
                    Map.Entry<Field, Integer> entry = iterator.next();
                    if (gbfieldtype != null) {
                        return new Tuple(getTupleDesc(), new Field[]{entry.getKey(), new IntField(what == Op.AVG ? entry.getValue() / cntMap.get(entry.getKey()) : entry.getValue())});
                    }else {
                        return new Tuple(getTupleDesc(), new Field[]{new IntField(what == Op.AVG ? entry.getValue() / cntMap.get(entry.getKey()) : entry.getValue())});
                    }

                } catch (NoSuchElementException e) {
                    return null;
                }
            }
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                super.open();
                iterator = aMap.entrySet().iterator();
            }

            @Override
            public OpIterator[] getChildren() {
                return new OpIterator[0];
            }

            @Override
            public void setChildren(OpIterator[] children) {

            }

            @Override
            public TupleDesc getTupleDesc() {
                if (gbfieldtype != null) {
                    return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                }
                else {
                    return new TupleDesc(new Type[]{Type.INT_TYPE});
                }
            }
        };
    }

}
