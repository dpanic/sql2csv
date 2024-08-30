import os
import sys
import csv
import shutil

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

def count_lines_in_file(filename):
    count_file = filename + '.count'
    if os.path.exists(count_file):
        with open(count_file, 'r') as f:
            total_lines = int(f.read().strip())
    else:
        total_lines = 0
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            for _ in f:
                total_lines += 1
        with open(count_file, 'w') as f:
            f.write(str(total_lines))
    return total_lines


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

    table_structures = {}
    buffer_size = 1000
    buffer = []
    line_count = 0
    insert_accumulator = ""
    in_insert_statement = False
    fds = {}
    total_lines = count_lines_in_file(input_file)
    stats_point = int(total_lines / 100)

    with open(input_file, 'r', encoding='utf-8', errors='replace') as infile:
        for line in infile:
            line_count += 1
            line = line.strip()

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
                        print(f"Warning: No structure found for table {table_name}")
            
            if line_count % stats_point == 0:
               print(f"Processed {line_count} lines out of {total_lines} ({(line_count / total_lines) * 100:.0f}%)... [current table: {table_name}] ")

        if len(buffer) > 0:
            csv_writer = fds[table_name]
            write_buffer_to_csv(buffer, csv_writer)

        print(f"Processing completed. Total lines processed: {line_count}")

process()
