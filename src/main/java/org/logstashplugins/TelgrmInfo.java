package org.logstashplugins;

public class TelgrmInfo {

    private String field;
    private int fieldSize;

    public TelgrmInfo(String field, int fieldSize) {
        this.field = field;
        this.fieldSize = fieldSize;
    }

    public String getField() {
        return field;
    }

    public int getFieldSize() {
        return fieldSize;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setFieldSize(int fieldSize) {
        this.fieldSize = fieldSize;
    }

    @Override
    public String toString() {
        return "field:'" + field + '\'' +
                ", fieldSize:" + fieldSize;
    }
}
