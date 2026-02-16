# MIT MEDSL Hiring Assignment

Data cleaning and analysis exercise for the Senior Research Support Associate position at MIT Election Data and Science Lab.

## Setup

Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run data cleaning (Part 1):
```bash
python src/clean_and_merge_data.py
```

This produces `data/processed/part1_clean.csv` with standardized precinct-level election data.

## Project Structure
```
.
├── data/
│   ├── raw/              # Input data
│   └── processed/        # Cleaned output
├── src/
│   └── clean_and_merge_data.py
├── requirements.txt
└── README.md
```

## Requirements

- Python 3.8+
- pandas, numpy