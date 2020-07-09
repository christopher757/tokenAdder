package mainPackage;

import com.univocity.parsers.common.record.Record;

public class CsvInputRecord {
    Record record;

    public CsvInputRecord(Record record) {
        this.record = record;
    }

    public String[] getValues() {
        return record.getValues();
    }

    public String getString(String column) {
        return record.getString(column);
    }

    public Long getLong(String column) {
        return record.getLong(column);
    }

    public Double getDouble(String column) {
        return record.getDouble(column);
    }

    public Float getFloat(String column) {
        return record.getFloat(column);
    }
}
