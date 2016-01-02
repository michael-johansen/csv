package net.michaeljohansen.csv;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvReader {
    private final Reader reader;
    private List<List<String>> lazyResult = null;
    private StringBuilder recordBuilder;
    private List<String> records;
    private State state;
    private List<List<String>> rows;
    private final Map<Operation, Map<State, Consumer<Character>>> operationStateMap = buildOperationMap();
    private final Map<Character, Operation> characterOperationMap;
    private static final Consumer<Character> NOOP = character -> {
    };

    public CsvReader(String inputString) {
        this(inputString, ',');
    }

    public CsvReader(String inputString, char delimiter) {
        this(new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8)), delimiter, StandardCharsets.UTF_8);
    }

    public CsvReader(InputStream inputStream) {
        this(inputStream, ',', StandardCharsets.UTF_8);
    }

    public CsvReader(InputStream inputStream, char delimiter, Charset charset) {
        reader = new InputStreamReader(inputStream, charset);
        characterOperationMap = buildCharacterOperationMap(delimiter);
    }

    public synchronized List<List<String>> getRows() {
        if (lazyResult == null) {
            state = State.OUTSIDE_VALUE;
            recordBuilder = new StringBuilder();
            clearRecords();
            rows = new ArrayList<>();
            int rawChar;
            while ((rawChar = softRead()) != -1) {
                char character = (char) rawChar;
                Operation operation = getOperation(character);
                Map<State, Consumer<Character>> stateMap = operationStateMap.get(operation);
                Consumer<Character> characterConsumer = stateMap.get(state);
                characterConsumer.accept(character);
            }
            close();
            this.lazyResult = rows;
        }
        return lazyResult;
    }

    public List<Map<String, String>> getRowsBasedOnHeaders() {
        List<List<String>> rows = getRows();
        if (rows.isEmpty()) throw new IllegalStateException("No lazyResult");
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

    private Operation getOperation(char character) {
        if (characterOperationMap.containsKey(character)) {
            return characterOperationMap.get(character);
        }
        return Operation.CHARACTER;
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

    private Map<Operation, Map<State, Consumer<Character>>> buildOperationMap() {
        Map<Operation, Map<State, Consumer<Character>>> map = new HashMap<>();
        map.put(Operation.QUOTE, getQuouteOperations());
        map.put(Operation.CHARACTER, getCharacterOperations());
        map.put(Operation.DELIMITER, getDelimeterOperations());
        map.put(Operation.NEWLINE, getNewlineOperations());
        map.put(Operation.IGNORE, getIgnoreOperations());
        return map;
    }

    private Map<State, Consumer<Character>> getIgnoreOperations() {
        Map<State, Consumer<Character>> ignoreOperations = new HashMap<>();
        ignoreOperations.put(State.INSIDE_QUOTED_VALUE, NOOP);
        ignoreOperations.put(State.INSIDE_VALUE, NOOP);
        ignoreOperations.put(State.QUOTE, NOOP);
        ignoreOperations.put(State.OUTSIDE_VALUE, NOOP);
        return ignoreOperations;
    }

    private Map<State, Consumer<Character>> getNewlineOperations() {
        Map<State, Consumer<Character>> newlineOperations = new HashMap<>();
        Consumer<Character> finishRecordAndNewline = character -> {
            state = State.OUTSIDE_VALUE;
            addBufferToRecords();
            appendRecordToRows();
            clearRecords();
        };
        newlineOperations.put(State.OUTSIDE_VALUE, finishRecordAndNewline);
        newlineOperations.put(State.QUOTE, finishRecordAndNewline);
        newlineOperations.put(State.INSIDE_VALUE, finishRecordAndNewline);
        newlineOperations.put(State.INSIDE_QUOTED_VALUE, (character) -> getRecordBuilder().append(character));
        return newlineOperations;
    }

    private Map<State, Consumer<Character>> getDelimeterOperations() {
        Map<State, Consumer<Character>> delimiterOperations = new HashMap<>();
        Consumer<Character> finishRecord = character -> {
            state = State.OUTSIDE_VALUE;
            addBufferToRecords();
        };
        delimiterOperations.put(State.OUTSIDE_VALUE, finishRecord);
        delimiterOperations.put(State.INSIDE_VALUE, finishRecord);
        delimiterOperations.put(State.QUOTE, finishRecord);
        delimiterOperations.put(State.INSIDE_QUOTED_VALUE, (character) -> getRecordBuilder().append(character));
        return delimiterOperations;
    }

    private Map<State, Consumer<Character>> getCharacterOperations() {
        Map<State, Consumer<Character>> characterOperations = new HashMap<>();
        characterOperations.put(State.OUTSIDE_VALUE, character -> {
            state = State.INSIDE_VALUE;
            getRecordBuilder().append(character);
        });
        characterOperations.put(State.INSIDE_VALUE, (character) -> getRecordBuilder().append(character));
        characterOperations.put(State.INSIDE_QUOTED_VALUE, (character) -> getRecordBuilder().append(character));
        characterOperations.put(State.QUOTE, character -> {
            throw new IllegalStateException("Met character after quote");
        });
        return characterOperations;
    }

    private Map<State, Consumer<Character>> getQuouteOperations() {
        Map<State, Consumer<Character>> quoteOperations = new HashMap<>();
        quoteOperations.put(State.OUTSIDE_VALUE, character -> state = State.INSIDE_QUOTED_VALUE);
        quoteOperations.put(State.INSIDE_QUOTED_VALUE, character -> state = State.QUOTE);
        quoteOperations.put(State.QUOTE, character -> {
            state = State.INSIDE_QUOTED_VALUE;
            getRecordBuilder().append(character);
        });
        quoteOperations.put(State.INSIDE_VALUE, character -> {
            throw new IllegalStateException(String.format(
                    "Met quote inside unescaped value, message so far [%s]",
                    recordBuilder
            ));
        });
        return quoteOperations;
    }

    private synchronized void clearRecords() {
        records = new ArrayList<>();
    }

    private synchronized void appendRecordToRows() {
        rows.add(records);
    }

    private synchronized void addBufferToRecords() {
        records.add(getRecordBuilder().toString());
        recordBuilder = new StringBuilder();
    }

    private StringBuilder getRecordBuilder() {
        return recordBuilder;
    }

    private Map<Character, Operation> buildCharacterOperationMap(char delimiter) {
        Map<Character, Operation> map = new HashMap<>();
        map.put(delimiter, Operation.DELIMITER);
        map.put('\n', Operation.NEWLINE);
        map.put('\r', Operation.IGNORE);
        map.put('"', Operation.QUOTE);
        return map;
    }

    enum State {
        OUTSIDE_VALUE,
        INSIDE_VALUE,
        INSIDE_QUOTED_VALUE,
        QUOTE
    }

    enum Operation {
        QUOTE,
        DELIMITER,
        NEWLINE,
        IGNORE,
        CHARACTER
    }
}