package flightproject;

import org.apache.hadoop.io.*;

/*
 * Format for Value Array [airline (Text), airport (Text), delayed (BooleanWritable), delayTime (FloatWritable), distance (FloatWritable)]
 */
public class FlightArray extends ArrayWritable {

    private static final int EXPECTED_LENGTH = 5;

    public FlightArray() {
        super(Writable.class);
    }

    public FlightArray(Text airline, Text airport, BooleanWritable delayed, FloatWritable delayTime, FloatWritable distance) {
        super(Writable.class, new Writable[]{airline, airport, delayed, delayTime, distance});
        validate(get());
    }

    public FlightArray(Writable[] values) {
        super(Writable.class, validate(values));
    }

    private static Writable[] validate(Writable[] values) {
        if (values.length != EXPECTED_LENGTH) {
            throw new IllegalArgumentException("FlightArray must have exactly " + EXPECTED_LENGTH + " elements");
        }
        if (!(values[0] instanceof Text && values[1] instanceof Text && values[2] instanceof BooleanWritable && values[3] instanceof FloatWritable && values[4] instanceof FloatWritable)) {
            throw new IllegalArgumentException("FlightArray elements must be of types Text, Text, BooleanWritable, FloatWritable, FloatWritable");
        }
        return values;
    }

    @Override
    public String toString() {
        Writable[] values = get();
        return String.format("Airline: %s, Airport: %s, Delayed: %b, Time Delayed: %.2f minutes, Distance: %.2f miles",
                ((Text) values[0]).toString(),
                ((Text) values[1]).toString(),
                ((BooleanWritable) values[2]).get(),
                ((FloatWritable) values[3]).get(),
                ((FloatWritable) values[4]).get());
    }
}
