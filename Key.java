package a_2;

public enum Key {
    /* fixed length fields */
    id(4, Integer.class),
    date_time(10, String.class),
    year(4, Integer.class),
    month(4, Integer.class),
    mdate(4, Integer.class),
    day(4, Integer.class),
    time(4, Integer.class),
    sensor_id(4, Integer.class),
    hourly_counts(4, Integer.class),

    /* non-fixed length fields */
    sensor_name(90, String.class),
    sdt_name(20, String.class);

    // the byte cost of key field in index file
    int byteCount;

    Class indexType;

    Key(int byteCount, Class indexType) {
        this.byteCount = byteCount;
        this.indexType = indexType;
    }
}
