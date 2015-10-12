package net.michaeljohansen.csv;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvReader {

    private final Reader reader;
    private final char delimiter;
    private List<List<String>> rows = null;

    public CsvReader(String inputString) {
        this(inputString, ',');
    }

    public CsvReader(String inputString, char delimiter) {
        reader = new StringReader(inputString);
        this.delimiter = delimiter;
    }

    public CsvReader(InputStream inputStream) {
        this(inputStream, ',', StandardCharsets.UTF_8);
    }

    public CsvReader(InputStream inputStream, char delimiter, Charset charset) {
        reader = new InputStreamReader(inputStream, charset);
        this.delimiter = delimiter;
    }

    public List<List<String>> getRows() {
        if (rows == null) {
            State state = State.OUTSIDE_VALUE;
            StringBuilder recordBuilder = new StringBuilder();
            List<String> records = new ArrayList<>();
            List<List<String>> rows = new ArrayList<>();
            int rawChar;
            while ((rawChar = softRead()) != -1) {
                char character = (char) rawChar;
                if (character == '"') {
                    switch (state) {
                        case OUTSIDE_VALUE:
                            state = State.INSIDE_QUOTED_VALUE;
                            break;
                        case INSIDE_QUOTED_VALUE:
                            state = State.QUOTE;
                            break;
                        case QUOTE:
                            state = State.INSIDE_QUOTED_VALUE;
                            recordBuilder.append(character);
                            break;
                        case INSIDE_VALUE:
                            throw new IllegalStateException("Met quote inside unescaped value");
                    }
                } else if (character == delimiter) {
                    switch (state) {
                        case OUTSIDE_VALUE:
                        case INSIDE_VALUE:
                        case QUOTE:
                            state = State.OUTSIDE_VALUE;
                            records.add(recordBuilder.toString());
                            recordBuilder = new StringBuilder();
                            break;
                        case INSIDE_QUOTED_VALUE:
                            recordBuilder.append(character);
                            break;
                    }
                } else if (character == '\n') {
                    switch (state) {
                        case OUTSIDE_VALUE:
                        case INSIDE_VALUE:
                        case QUOTE:
                            state = State.OUTSIDE_VALUE;
                            records.add(recordBuilder.toString());
                            recordBuilder = new StringBuilder();
                            rows.add(records);
                            records = new ArrayList<>();
                            break;
                        case INSIDE_QUOTED_VALUE:
                            recordBuilder.append(character);
                            break;
                    }
                } else {
                    switch (state) {
                        case OUTSIDE_VALUE:
                            state = State.INSIDE_VALUE;
                            recordBuilder.append(character);
                            break;
                        case INSIDE_VALUE:
                            recordBuilder.append(character);
                            break;
                        case INSIDE_QUOTED_VALUE:
                            recordBuilder.append(character);
                            break;
                        case QUOTE:
                            throw new IllegalStateException("Met character after quote");
                    }
                }
            }
            close();
            this.rows = rows;
        }
        return rows;
    }

    public List<Map<String, String>> getRowsBasedOnHeaders() {
        List<List<String>> rows = getRows();
        if (rows.isEmpty()) throw new IllegalStateException("No rows");
        List<String> strings = rows.remove(0);
        return rows.stream().map(
                row -> {
                    Map<String, String> map = new HashMap<>();
                    IntStream.range(0, strings.size())
                            .forEach(index -> map.put(strings.get(index), row.get(index)));
                    return map;
                }
        ).collect(Collectors.toList());
    }

    private int softRead() {
        try {
            return reader.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    enum State {
        OUTSIDE_VALUE,
        INSIDE_VALUE,
        INSIDE_QUOTED_VALUE,
        QUOTE

    }
}
