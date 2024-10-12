import os
import sys
import csv
import shutil
import chardet

def extract_column_names(line):
    line = line.split(" ")[0] 
    line = line.replace("`", "") 
    return line

def write_buffer_to_csv(buffer, csv_writer):
    if csv_writer is not None:
        csv_writer.writerows(buffer)
    buffer.clear()

def parse_sql_insert(insert_line):
    try:
        table_name, values_part = insert_line.split(" VALUES ", 1)
        table_name = table_name.split()[2].strip('`')

        values_part = values_part.rstrip(";")  # Remove the trailing semicolon
        values = values_part.split("),(")

        parsed_data = []
        for val in values:
            # Strip the parentheses around the value
            val = val.strip("()")
            
            # Split the value into individual elements
            elements = val.split(',')
            
            # Strip quotes and whitespace from each element
            for e in range(len(elements)):
                prev_size = -1
                while 1:
                    elements[e] = elements[e].strip(' ')
                    elements[e] = elements[e].strip('"')
                    elements[e] = elements[e].strip("'")
                    elements[e] = elements[e].strip('\t')

                    if prev_size == len(elements[e]):
                        break
                    prev_size = len(elements[e])
            parsed_data.append(elements)

        return table_name, parsed_data

    except Exception as e:
        print(f"Error parsing line: {insert_line}")
        raise e

def count_lines_in_file(filename, encoding):
    count_file = filename + '.count'
    if os.path.exists(count_file):
        with open(count_file, 'r') as f:
            total_lines = int(f.read().strip())
    else:
        total_lines = 0
        with open(filename, 'r', encoding=encoding, errors='replace') as f:
            for _ in f:
                total_lines += 1
        with open(count_file, 'w') as f:
            f.write(str(total_lines))
    return total_lines

def detect_encoding(filename, size=1024*1024):
    with open(filename, 'rb') as f:
        raw_data = f.read(size)  # Read only up to 'size' bytes
    result = chardet.detect(raw_data)
    return result['encoding']

def process():
    if len(sys.argv) != 2:
        print("Usage: python sql_to_csv.py <filename>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = os.path.splitext(input_file)[0] + '_csv_output'
    
    try:
        shutil.rmtree(output_dir)
    except:
        pass
    os.makedirs(output_dir, exist_ok=True)

    # Detect file encoding
    file_encoding = detect_encoding(input_file)
    print(f"Detected file encoding: {file_encoding}")

    table_structures = {}
    buffer_size = 1000
    buffer = []
    line_count = 0
    insert_accumulator = ""
    in_insert_statement = False
    fds = {}
    total_lines = count_lines_in_file(input_file, file_encoding)
    stats_point = int(total_lines / 100)
    table_name = None  # Initialize table_name to avoid UnboundLocalError

    with open(input_file, 'r', encoding=file_encoding, errors='replace') as in_file:
        for line in in_file:
            line_count += 1
            line = line.strip()

            if line.startswith('--') or line == "":
                continue

            if line.startswith("CREATE TABLE"):
                table_name = line.split()[2].strip('`')
                table_structures[table_name] = []

                output_file = os.path.join(output_dir, f"{table_name}.csv")
                csv_file = open(output_file, 'w', newline='', encoding='utf-8')
                csv_writer = csv.writer(csv_file, delimiter='\t')
                csv_writer.writerow(table_structures[table_name])
                fds[table_name] = csv_writer

            elif line.lstrip().startswith("`"):
                column_name = extract_column_names(line)
                table_structures[table_name].append(column_name)

            elif line.startswith("INSERT INTO") or in_insert_statement:
                line = line.replace('\r\n', ' ')
                line = line.replace('\n', ' ')
                line = line.replace('\r', ' ')
                line = line.strip(' ')

                if line.endswith(" VALUES"):
                    line += " "
                insert_accumulator += line

                in_insert_statement = True

                if line.endswith(";"):
                    table_name, parsed_data = parse_sql_insert(insert_accumulator)

                    insert_accumulator = ""
                    in_insert_statement = False

                    if table_name in table_structures:
                        buffer.extend(parsed_data)

                        if len(buffer) > buffer_size:
                            csv_writer = fds[table_name]
                            write_buffer_to_csv(buffer, csv_writer)
                    else:
                        # Fallback mechanism when no structure is detected
                        if table_name not in fds:
                            # Open new CSV file and create a dynamic header
                            output_file = os.path.join(output_dir, f"{table_name}_fallback.csv")
                            csv_file = open(output_file, 'w', newline='', encoding='utf-8')
                            csv_writer = csv.writer(csv_file, delimiter='\t')

                            # Create header dynamically based on field count in parsed_data
                            max_fields = max(len(row) for row in parsed_data)
                            dynamic_header = [f"field_{i+1}" for i in range(max_fields)]
                            csv_writer.writerow(dynamic_header)
                            fds[table_name] = csv_writer

                        buffer.extend(parsed_data)

                        if len(buffer) > buffer_size:
                            csv_writer = fds[table_name]
                            write_buffer_to_csv(buffer, csv_writer)

            if line_count % stats_point == 0:
                current_table = table_name if table_name else "Unknown"
                print(f"Processed {line_count} lines out of {total_lines} ({(line_count / total_lines) * 100:.0f}%)... [current table: {current_table}] ")

        if len(buffer) > 0:
            csv_writer = fds[table_name]
            write_buffer_to_csv(buffer, csv_writer)

        print(f"Processing completed. Total lines processed: {line_count}")

process()
