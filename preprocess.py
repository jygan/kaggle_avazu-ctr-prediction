import os
import sys
if __name__ == "__main__":
    input_file = sys.argv[1]
    preprocess_file = sys.argv[2]
    test_flag = sys.argv[3]
    print "input=" + input_file
    print "preprocess_file=" + preprocess_file
    output = open(preprocess_file, "w")
    with open(input_file, "r") as lines:
            next(lines)
            for line in lines:
                fields = line.split(",")
                index = 5
                end = 14
                if test_flag == "1":
                    index = 4
                    end = 13
                while index < end:
                    fields[index] = str(hash(fields[index]) % 1000000)
                    index += 1

                newline = ",".join(fields)
                output.write(newline)


    output.close()
