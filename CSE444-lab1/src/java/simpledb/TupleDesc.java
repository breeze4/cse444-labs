package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private final List<TDItem> items = new ArrayList<>();

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

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}

		protected int getByteSize() {
			int typeSize = fieldType != null ? fieldType.getLen() : 0;
			return typeSize;
		}
	}

	/**
	 * @return An iterator which iterates over all the field TDItems that are
	 *         included in this TupleDesc
	 * */
	public Iterator<TDItem> iterator() {
		// some code goes here
		return items.iterator();
	}

	private static final long serialVersionUID = 1L;

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
		// some code goes here
		List<Type> types = Arrays.asList(typeAr);
		List<String> fields = Arrays.asList(fieldAr);

		Iterator<Type> typesIter = types.iterator();
		Iterator<String> fieldsIter = fields.iterator();
		while (typesIter.hasNext() && fieldsIter.hasNext()) {
			TDItem item = new TDItem(typesIter.next(), fieldsIter.next());
			items.add(item);
		}
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
		// some code goes here
		List<Type> types = Arrays.asList(typeAr);

		Iterator<Type> typesIter = types.iterator();
		while (typesIter.hasNext()) {
			TDItem item = new TDItem(typesIter.next(), null);
			items.add(item);
		}
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// some code goes here
		return items.size();
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
		// some code goes here
		try {
			TDItem item = items.get(i);
			return item.fieldName;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
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
		// some code goes here
		try {
			TDItem item = items.get(i);
			if (item.fieldType == null) {
				throw new NoSuchElementException();
			} else {
				return item.fieldType;
			}
		} catch (ArrayIndexOutOfBoundsException e) {
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
		// some code goes here
		for (int i = 0; i < items.size(); i++) {
			TDItem item = items.get(i);
			if (Objects.equals(item.fieldName, name)) {
				return i;
			}
		}
		throw new NoSuchElementException();
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		// some code goes here
		int runningTotal = 0;
		for (TDItem item : items) {
			runningTotal += item.getByteSize();
		}
		return runningTotal;
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
		// some code goes here
		if (td1 == null && td2 != null) {
			return td2;
		} else if (td1 != null && td2 == null) {
			return td1;
		} else if (td1 == null && td2 == null) {
			return null;
		}

		int td1Length = td1.numFields();
		int mergedLength = td1Length + td2.numFields();
		Type[] mergedTypes = new Type[mergedLength];
		String[] mergedNames = new String[mergedLength];
		for (int i = 0; i < td1Length; i++) {
			mergedTypes[i] = td1.getFieldType(i);
			mergedNames[i] = td1.getFieldName(i);
		}
		for (int i = 0; i < td2.numFields(); i++) {
			mergedTypes[i + td1Length] = td2.getFieldType(i);
			mergedNames[i + td1Length] = td2.getFieldName(i);
		}

		TupleDesc td = new TupleDesc(mergedTypes, mergedNames);
		return td;
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 * 
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {
		// some code goes here
		if(!(o instanceof TupleDesc)) {
			return false;
		}
		TupleDesc td = (TupleDesc) o;
		// check size
		if(td.numFields() != this.numFields())
			return false;
		// check elements
		for (int i = 0; i < td.items.size(); i++) {
			if(!Objects.equals(this.getFieldName(i), td.getFieldName(i)) ||
					!Objects.equals(this.getFieldType(i), td.getFieldType(i))) {
				return false;
			}
		}
		return true;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	@Override
	public String toString() {
		StringJoiner sj = new StringJoiner(",");
		for (TDItem item : items) {
			sj.add(item.toString());
		}
		return sj.toString();
	}
//	public String toString() {
//		// some code goes here
//		return "";
//	}
}
