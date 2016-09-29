package net.michaeljohansen.csv;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class CsvReader {
  private static final Consumer<Character> NO_OP = character -> {
  };
  private final Reader reader;
  private final Map<Character, Operation> characterOperationMap;
  private List<List<String>> lazyResult = null;
  private StringBuilder recordBuilder;
  private List<String> records;
  private State state;
  private List<List<String>> rows;
  private final Map<Operation, Map<State, Consumer<Character>>> operationStateMap = buildOperationMap();

  public CsvReader(String inputString) {
    this(inputString, ',');
  }

  public CsvReader(String inputString, char delimiter) {
    this(
        new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8)),
        delimiter,
        StandardCharsets.UTF_8
    );
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
        Consumer<Character> characterConsumer = getCharacterConsumer(operation);
        characterConsumer.accept(character);
      }
      finishOpenEntity();
      close();
      this.lazyResult = rows;
    }
    return lazyResult;
  }

  private void finishOpenEntity() {
    if (!records.isEmpty()) {
      getCharacterConsumer(getOperation('\n')).accept('\n');
    }
  }

  private Consumer<Character> getCharacterConsumer(Operation operation) {
    Map<State, Consumer<Character>> stateMap = operationStateMap.get(
        operation
    );
    if (stateMap == null){
      throw new IllegalStateException(String.format(
          "no stateMap for operation: %s",
          operation
      ));
    }
    Consumer<Character> characterConsumer = stateMap.get(state);
    if (characterConsumer == null){
      throw new IllegalStateException(String.format(
          "no characterConsumer for operation: %s and state: %s",
          operation,
          state
      ));
    }
    return characterConsumer;
  }

  public List<Map<String, String>> getRowsBasedOnHeaders() {
    List<List<String>> rows = getRows();
    if (rows.isEmpty()) {
      throw new IllegalStateException("No lazyResult");
    }
    List<String> strings = rows.remove(0);
    return rows.stream()
        .map(row -> IntStream.range(0, strings.size())
            .boxed()
            .collect(toMap(strings::get, row::get))
        )
        .collect(toList());
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
    map.put(Operation.QUOTE, getQuoteOperations());
    map.put(Operation.CHARACTER, getCharacterOperations());
    map.put(Operation.DELIMITER, getDelimiterOperations());
    map.put(Operation.NEWLINE, getNewlineOperations());
    map.put(Operation.IGNORE, getIgnoreOperations());
    map.put(Operation.START_COMMENT, getCommentOperations());
    return map;
  }

  private Map<State, Consumer<Character>> getIgnoreOperations() {
    Map<State, Consumer<Character>> operations = new HashMap<>();
    operations.put(State.INSIDE_QUOTED_VALUE, NO_OP);
    operations.put(State.INSIDE_VALUE, NO_OP);
    operations.put(State.QUOTE, NO_OP);
    operations.put(State.OUTSIDE_VALUE, NO_OP);
    operations.put(State.COMMENT, NO_OP);
    return operations;
  }

  private Map<State, Consumer<Character>> getNewlineOperations() {
    Map<State, Consumer<Character>> operations = new HashMap<>();
    Consumer<Character> finishRecordAndNewline = character -> {
      state = State.OUTSIDE_VALUE;
      addBufferToRecords();
      appendRecordToRows();
      clearRecords();
    };
    operations.put(State.OUTSIDE_VALUE, finishRecordAndNewline);
    operations.put(State.QUOTE, finishRecordAndNewline);
    operations.put(State.INSIDE_VALUE, finishRecordAndNewline);
    operations.put(
        State.INSIDE_QUOTED_VALUE,
        (character) -> getRecordBuilder().append(character)
    );
    operations.put(State.COMMENT, character -> state = State.OUTSIDE_VALUE);
    return operations;
  }

  private Map<State, Consumer<Character>> getDelimiterOperations() {
    Map<State, Consumer<Character>> operations = new HashMap<>();
    Consumer<Character> finishRecord = character -> {
      state = State.OUTSIDE_VALUE;
      addBufferToRecords();
    };
    operations.put(State.OUTSIDE_VALUE, finishRecord);
    operations.put(State.INSIDE_VALUE, finishRecord);
    operations.put(State.QUOTE, finishRecord);
    operations.put(
        State.INSIDE_QUOTED_VALUE,
        (character) -> getRecordBuilder().append(character)
    );
    operations.put(State.COMMENT, NO_OP);
    return operations;
  }

  private Map<State, Consumer<Character>> getCharacterOperations() {
    Map<State, Consumer<Character>> operations = new HashMap<>();
    operations.put(State.OUTSIDE_VALUE, character -> {
      state = State.INSIDE_VALUE;
      getRecordBuilder().append(character);
    });
    operations.put(
        State.INSIDE_VALUE,
        (character) -> getRecordBuilder().append(character)
    );
    operations.put(
        State.INSIDE_QUOTED_VALUE,
        (character) -> getRecordBuilder().append(character)
    );
    operations.put(State.QUOTE, character -> {
      throw new IllegalStateException("Met character after quote");
    });
    operations.put(State.COMMENT, NO_OP);
    return operations;
  }

  private Map<State, Consumer<Character>> getQuoteOperations() {
    Map<State, Consumer<Character>> operations = new HashMap<>();
    operations.put(
        State.OUTSIDE_VALUE,
        character -> state = State.INSIDE_QUOTED_VALUE
    );
    operations.put(
        State.INSIDE_QUOTED_VALUE,
        character -> state = State.QUOTE
    );
    operations.put(State.QUOTE, character -> {
      state = State.INSIDE_QUOTED_VALUE;
      getRecordBuilder().append(character);
    });
    operations.put(State.INSIDE_VALUE, character -> {
      throw new IllegalStateException(String.format(
          "Met quote inside unescaped value, message so far [%s]",
          recordBuilder
      ));
    });
    operations.put(State.COMMENT, NO_OP);
    return operations;
  }

  private Map<State, Consumer<Character>> getCommentOperations() {
    // Consumed as normal, if not outside value
    Map<State, Consumer<Character>> operations = getCharacterOperations();
    operations.put(
        State.OUTSIDE_VALUE,
        character -> state = State.COMMENT
    );
    return operations;
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
    map.put('#', Operation.START_COMMENT);
    return map;
  }

  enum State {
    OUTSIDE_VALUE,
    INSIDE_VALUE,
    INSIDE_QUOTED_VALUE,
    COMMENT,
    QUOTE
  }

  enum Operation {
    QUOTE,
    DELIMITER,
    NEWLINE,
    IGNORE,
    START_COMMENT,
    CHARACTER
  }
}
