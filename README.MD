# CSV

This is a small CSVReader based on the rules for csv on [wikipedia](https://en.wikipedia.org/wiki/Comma-separated_values)
 
## Features
* Plain values
 * `new CsvReader("1997,Ford,E350\n")`
* Quoted values
 * `new CsvReader("1997,Ford,E350,\"Go get one now\nthey are going fast\"\n")`
* Basic Windows/Unix-style line endings support
 * `new CsvReader("1\r\n,2\r\n")`
* User can set preferred delimiter
 * `new CsvReader("1997;Ford;E350\n", ';')`
* Significant whitespaces
 * `new CsvReader("1997, Ford, E350\n")`
* Read headers from first value row
 * `new CsvReader("Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n").getRowsBasedOnHeaders()`
* Comments
 * `new CsvReader("#1\n2\n#3\n4\n")`


