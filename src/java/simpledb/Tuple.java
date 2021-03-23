package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private TupleDesc tupleDesc;

    private RecordId recordId;

    private List<Field> fieldList;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        this.fieldList = new ArrayList<>();
        for (int i = 0; i < td.numFields(); ++i) {
            this.fieldList.add(new IntField(-1));
        }
    }

    public Tuple(TupleDesc td, Field[] fields) {
        this.tupleDesc = td;
        this.fieldList = new ArrayList<>();
        for (int i = 0; i < td.numFields(); ++i) {
            this.fieldList.add(fields[i]);
        }
    }

    public Tuple(Tuple tup1, Tuple tup2) {
        this.tupleDesc = TupleDesc.merge(tup1.tupleDesc, tup2.tupleDesc);
        this.fieldList = new ArrayList<>(tup1.fieldList);
        this.fieldList.addAll(tup2.fieldList);
    }

    public Tuple(Tuple tuple) {
        this(tuple, new Tuple(new TupleDesc(new Type[]{})));
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        this.fieldList.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return this.fieldList.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        final int len = this.fieldList.size();
        for (int i = 0; i < len - 1; ++i) {
            sb.append(this.fieldList.get(i).toString()).append(i + 1).append("\t");
        }
        sb.append(this.fieldList.get(len - 1).toString()).append(len);
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Tuple)) {
            return false;
        }
        Tuple tObj = (Tuple) obj;
        return tObj.toString().equals(this.toString());
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return this.fieldList.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.tupleDesc = td;
    }
}
