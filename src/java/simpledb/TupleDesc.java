package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> descriptors = new ArrayList<>();

    private int size;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public int hashCode() {
            return this.toString().hashCode();
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof TDItem)) {
                return false;
            }
            TDItem TDObj = (TDItem) obj;
            return this.fieldType == TDObj.fieldType && TDObj.fieldName.equals(this.fieldName);
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return descriptors.iterator();
    }

    private static final long serialVersionUID = 1L;

    private void computeSize() {
        int total = 0;
        for (TDItem t : descriptors) {
            total += t.fieldType.getLen();
        }
        size = total;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        final int len = typeAr.length;
        for (int i = 0; i < len; ++i) {
            descriptors.add(new TDItem(typeAr[i], fieldAr[i]));
        }
        computeSize();
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        final int len = typeAr.length;
        for (int i = 0; i < len; ++i) {
            descriptors.add(new TDItem(typeAr[i], "unnamed" + i));
        }
        computeSize();
    }

    private TupleDesc(List<TDItem> descriptors) {
        this.descriptors = descriptors;
        computeSize();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return descriptors.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return descriptors.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        try {
            return descriptors.get(i).fieldType;
        } catch (Exception e) {
            throw new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException();
        }
        int i;
        for (i = 0; i < descriptors.size(); ++i) {
            if (name.equals(descriptors.get(i).fieldName)) {
                break;
            }
        }
        if (i == descriptors.size()) {
            throw new NoSuchElementException();
        }
        return i;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<TDItem> mergeList = new ArrayList<>(td1.descriptors);
        mergeList.addAll(td2.descriptors);
        return new TupleDesc(new ArrayList<>(mergeList));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc oT = (TupleDesc) o;
        final int otLen = oT.numFields();
        if (otLen != descriptors.size()) {
            return false;
        }
        for (int i = 0; i < otLen; ++i) {
            if (oT.descriptors.get(i).fieldType != descriptors.get(i).fieldType) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        return this.toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        final int len = descriptors.size();
        for (int i = 0; i < len; ++i) {
            sb.append("fieldType[").append(i).append("](").append("fieldName[").append(i).append("]),");
        }
        return sb.toString();
    }
}
