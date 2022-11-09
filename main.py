import concurrent.futures
from pathlib import Path

import pandas as pd


def create_output_path(input_path:Path):
    output_path = Path(str(input_path).replace('csv', 'processed'))
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path

def get_output_file_path(output_path:Path, input_file_path:Path):
    return Path(output_path, input_file_path.name)

def run_patient(input_path:Path):
    output_path = create_output_path(input_path)
    for file1, file2 in zip(Path(input_path, 'AWP').glob("*.csv"), Path(input_path, 'AWF').glob("*.csv")):
        output_file_path = get_output_file_path(output_path, file1)
        output = pd.merge(pd.read_csv(file1), pd.read_csv(file2))
        output.to_csv(output_file_path, index = False)

def main():
    ajou_data_path = Path("/home/ubuntu/2022-nia-mv/아주대/csv")
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        executor.map(run_patient, ajou_data_path.glob("**/2-*"))

    yonsei_data_path = Path("/home/ubuntu/2022-nia-mv/연세대/csv")
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        executor.map(run_patient, yonsei_data_path.glob("**/4-*"))
        
if __name__ == "__main__":
    main()