import json
def format_json(input_file, output_file, indent=4):
    try:
        with open(input_file, 'r') as infile:
            data = json.load(infile)
        with open(output_file, 'w') as outfile:
            json.dump(data, outfile, indent=indent)
        print(f"Formatted JSON data has been saved to '{output_file}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
if __name__ == "__main__":
    input_file = "articles.json"
    output_file = "results.json"
    format_json(input_file, output_file)