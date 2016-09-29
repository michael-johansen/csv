package net.michaeljohansen.csv;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class CsvReaderTest {
  @Test
  public void adjacentFieldsAreSeparatedWithComma() {
    CsvReader csvReader = new CsvReader("1997,Ford,E350\n");
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(1));

    List<String> rowValues = rows.get(0);
    assertThat(rowValues, hasItems("1997", "Ford", "E350"));
  }

  @Test
  public void canHandleWindowNewline() {
    CsvReader csvReader = new CsvReader("1\r\n,2\r\n");
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(2));

    assertThat(rows.get(0), hasItems("1"));
    assertThat(rows.get(1), hasItems("2"));
  }

  @Test
  public void shouldNotRequireTerminatingNewLine() {
    CsvReader csvReader = new CsvReader("1,\r\n,2,");
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(2));

    assertThat(rows.get(0), hasItems("1"));
    assertThat(rows.get(1), hasItems("2"));
  }

  @Test
  public void adjacentFieldsAreSeparatedWithCommaWithStream() {
    CsvReader csvReader = new CsvReader(new ByteArrayInputStream(
        "1997,Ford,E350\n".getBytes(StandardCharsets.UTF_8)));
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(1));

    List<String> rowValues = rows.get(0);
    assertThat(rowValues, hasItems("1997", "Ford", "E350"));
  }

  @Test
  public void adjacentFieldsAreSeparatedWithSemicolon() {
    CsvReader csvReader = new CsvReader("1997;Ford;E350\n", ';');
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(1));

    List<String> rowValues = rows.get(0);
    assertThat(rowValues, hasItems("1997", "Ford", "E350"));
  }

  @Test
  public void anyFieldMayBeQuoted() {
    CsvReader csvReader = new CsvReader("\"1997\",\"Ford\",\"E350\"\n");
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(1));

    List<String> rowValues = rows.get(0);
    assertThat(rowValues, hasItems("1997", "Ford", "E350"));
  }

  @Test
  public void fieldsWithEmbeddedCommasOrDoubleQuoteCharactersMustBeQuoted() {
    CsvReader csvReader = new CsvReader(
        "1997,Ford,E350,\"Super, luxurious truck\"\n"
    );
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(1));

    List<String> rowValues = rows.get(0);
    assertThat(
        rowValues,
        hasItems("1997", "Ford", "E350", "Super, luxurious truck")
    );
  }

  @Test
  public void shouldAcceptQuoteInValues() {
    CsvReader csvReader = new CsvReader(
        "1997,Ford,E350,\"Super, \"\"luxurious\"\" truck\"\n"
    );
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(1));

    List<String> rowValues = rows.get(0);
    assertThat(
        rowValues,
        hasItems(
            "1997",
            "Ford",
            "E350",
            "Super, \"luxurious\" truck"
        )
    );
  }

  @Test
  public void fieldsWithEmbeddedLineBreaksMustBeQuoted() {
    CsvReader csvReader = new CsvReader(
        "1997,Ford,E350,\"Go get one now\nthey are going fast\"\n"
    );
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(1));

    List<String> rowValues = rows.get(0);
    assertThat(
        rowValues,
        hasItems("1997", "Ford", "E350", "Go get one now\nthey are going fast")
    );
  }

  @Test
  public void spacesAreConsideredPartOfAFieldAndShouldNotBeIgnored() {
    CsvReader csvReader = new CsvReader("1997, Ford, E350\n");
    List<List<String>> rows = csvReader.getRows();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(1));

    List<String> rowValues = rows.get(0);
    assertThat(rowValues, hasItems("1997", " Ford", " E350"));
  }

  @Test(expected = IllegalStateException.class)
  public void quotesInAFieldAreNotAllowed() {
    CsvReader csvReader = new CsvReader("1997, \"Ford\" ,E350\n");
    csvReader.getRows();
  }

  @Test
  public void canUseHeaderRowToOrganizeValue() {
    CsvReader csvReader = new CsvReader(
        "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n"
    );
    List<Map<String, String>> rows = csvReader.getRowsBasedOnHeaders();

    assertThat(rows, is(notNullValue()));
    assertThat(rows.size(), is(2));

    Map<String, String> row0 = rows.get(0);
    assertThat(row0.get("Year"), is("1997"));
    assertThat(row0.get("Make"), is("Ford"));
    assertThat(row0.get("Model"), is("E350"));

    Map<String, String> row1 = rows.get(1);
    assertThat(row1.get("Year"), is("2000"));
    assertThat(row1.get("Make"), is("Mercury"));
    assertThat(row1.get("Model"), is("Cougar"));
  }

  @Test
  public void shouldIgnoreLinesThatStartWithHash() {
    List<List<String>> rows = new CsvReader(
        "#1\n2\n#3\n4\n"
    ).getRows();

    assertThat(rows.size(), is(2));
    assertThat(rows, hasItems(
        hasItems("2"),
        hasItems("4")
    ));
  }

  @Test
  public void shouldIgnoreLeadingCommentWhenChoosingHeader() {
    List<Map<String, String>> rows = new CsvReader(
        "#licence-comment\nnumber\n#1\n2\n#3\n4\n"
    ).getRowsBasedOnHeaders();

    assertThat(rows.size(), is(2));

    List<String> numbers = rows.stream()
        .map(stringStringMap -> stringStringMap.get("number"))
        .collect(toList());
    assertThat(numbers, hasItems("2", "4"));
    assertThat(numbers, not(hasItem("1")));
    assertThat(numbers, not(hasItem("3")));

  }
}
