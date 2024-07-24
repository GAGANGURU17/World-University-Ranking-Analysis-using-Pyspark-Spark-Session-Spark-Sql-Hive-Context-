# World University Rankings Analysis

## Project Overview
This project analyzes world university rankings using PySpark, focusing on data from Times Higher Education, Center for World University Rankings (CWUR), and Academic Ranking of World Universities (Shanghai Ranking). The analysis provides insights into global higher education trends, institutional performance, and comparative standings across different ranking systems.

## Dataset
This project uses the World University Rankings dataset from Kaggle:
https://www.kaggle.com/datasets/mylesoneill/world-university-rankings

The dataset includes:
- Times Higher Education World University Rankings
- Center for World University Rankings (CWUR)
- Academic Ranking of World Universities (Shanghai Ranking)

To use this dataset:
1. Download the CSV files from Kaggle
2. Place the files in the `data/` directory of this project
3. Ensure you have `timesData.csv`, `cwurData.csv`, and `shanghaiData.csv` in the data directory

## Features
- Data loading and preprocessing using PySpark
- Comparative analysis across different ranking systems
- Trend analysis of university performance over time
- Country-wise performance analysis
- Analysis of specific metrics such as teaching quality, research impact, and international outlook
- Visualization of key insights (using matplotlib or other visualization libraries)

## Requirements
- Python 3.7+
- PySpark 3.1.2
- Pytest 6.2.5
- Pandas 1.3.3
- Matplotlib 3.4.3

## Setup
1. Clone the repository:
   ```
   git clone https://github.com/GAGANGURU17/world-university-rankings-analysis.git
   cd world-university-rankings-analysis
   ```
2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
4. Download the dataset from Kaggle and place the CSV files in the `data/` directory

## Usage
Run the main analysis script:
```
python src/main.py
```

To run tests:
```
pytest tests/
```

## Project Structure
```
world-university-rankings-analysis/
├── data/                   # Dataset files (not included in repo)
├── src/
│   ├── main.py             # Main script to run the analysis
│   ├── data_loading.py     # Functions for loading and preprocessing data
│   ├── analysis.py         # Analysis functions
│   └── utils.py            # Utility functions
├── tests/
│   └── test_analysis.py    # Unit tests for analysis functions
├── notebooks/              # Jupyter notebooks for exploratory data analysis
├── results/                # Directory to store analysis results and visualizations
├── requirements.txt        # Project dependencies
├── README.md               # Project documentation
└── .gitignore              # Git ignore file
```

## Analysis Components
1. **Times Higher Education Rankings Analysis**
   - Top universities globally
   - Country-wise average scores
   - International outlook analysis
   - Research impact analysis
   - Teaching quality analysis

2. **CWUR Rankings Analysis**
   - Top universities by CWUR standards
   - Alumni employment analysis

3. **Shanghai Rankings Analysis**
   - Top universities according to Shanghai Ranking
   - Research output analysis

4. **Comparative Analysis**
   - Universities present in top 100 of all three ranking systems
   - Comparison of ranking methodologies

5. **Trend Analysis**
   - Performance trends of universities over the years
   - Evolution of different metrics (e.g., research impact, teaching quality)

6. **Country Performance Analysis**
   - Overall country performance in global rankings
   - Strengths and weaknesses of different national higher education systems

## Contributing
Contributions to this project are welcome. Please follow these steps:
1. Fork the repository
2. Create a new branch (`git checkout -b feature/AmazingFeature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
5. Push to the branch (`git push origin feature/AmazingFeature`)
6. Open a Pull Request

## License
Distributed under the MIT License. See `LICENSE` file for more information.

## Contributors
- Venkata Reddy Kovvuri
- Venkata Naga Sri Sahithi Mulukutla
- Gagan N
- Yaswant Vuppalapati

## Contact
For any queries regarding this project, please open an issue on GitHub.

Project Link: https://github.com/GAGANGURU17/world-university-rankings-analysis
