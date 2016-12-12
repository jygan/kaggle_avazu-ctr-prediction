import sys

import os
import sys
if __name__ == "__main__":
    input_file = sys.argv[1]
    train_file = sys.argv[2]
    test_file = sys.argv[3]
    test_start_date = sys.argv[4]
    test_data_ouput = open(test_file, "w")
    train_data_output = open(train_file, "w")


    with open(input_file, "r") as lines:
            next(lines)
            for line in lines:
                fields = line.split(",")
                newline = ",".join(fields)
                if fields[2].startswith(test_start_date):
                    test_data_ouput.write(newline)
                else:
                    train_data_output.write(newline)


    train_data_output.close()
    test_data_ouput.close()
