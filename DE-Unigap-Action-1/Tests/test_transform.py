#%%
import sys
import os
import pytest
import pandas as pd

# Add path để import etl.transform
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.transform import (
    extract_salary_range,
    normalize_job_title,
    split_city_district_dynamic, 
    fuzzy_match_job_title
)


# ==== Test extract_salary_range ====

def test_extract_salary_range_normal_range():
    s = "10 - 20 triệu"
    min_s, max_s, unit = extract_salary_range(s)
    assert min_s == 10
    assert max_s == 20
    assert unit == "VND"

def test_extract_salary_range_usd():
    s = "5000 USD"
    min_s, max_s, unit = extract_salary_range(s)
    assert min_s == 5000
    assert max_s == 5000
    assert unit == "USD"

def test_extract_salary_range_thoathuan():
    s = "Thoả thuận"
    min_s, max_s, unit = extract_salary_range(s)
    assert min_s is None
    assert max_s is None
    assert unit == "Other"

def test_extract_salary_range_toi():
    s = "Tới 35 triệu"
    min_s, max_s, unit = extract_salary_range(s)
    assert min_s is None
    assert max_s == 35
    assert unit == "VND"

def test_extract_salary_range_null():
    s = None
    min_s, max_s, unit = extract_salary_range(s)
    assert min_s is None
    assert max_s is None
    assert unit == "Unknown"

# ==== Test normalize_job_title ====

def test_normalize_job_title_data():
    title = "Business Intelligence Analyst"
    assert normalize_job_title(title) == "Data"

def test_normalize_job_title_intern():
    title = "Thực tập sinh lập trình"
    assert normalize_job_title(title) == "Intern"

def test_normalize_job_title_unknown():
    title = "Chuyên viên kinh doanh"
    assert normalize_job_title(title) == "Other"

# ==== Test split_city_district_dynamic ====

def test_split_city_district():
    city_set = {"Hà Nội", "Hồ Chí Minh", "Đà Nẵng"}
    address = "Hà Nội: Cầu Giấy: Hồ Chí Minh: Quận 1"
    result = split_city_district_dynamic(address, city_set)
    assert result["city_1"] == "Hà Nội"
    assert result["district_1"] == "Cầu Giấy"
    assert result["city_2"] == "Hồ Chí Minh"
    assert result["district_2"] == "Quận 1"

def test_split_city_district_missing_district():
    city_set = {"Hà Nội"}
    address = "Hà Nội"
    result = split_city_district_dynamic(address, city_set)
    assert result["city_1"] == "Hà Nội"
    assert result["district_1"] is None

def test_split_city_district_invalid_address():
    city_set = {"Hà Nội", "Đà Nẵng"}
    address = None
    result = split_city_district_dynamic(address, city_set)
    assert isinstance(result, pd.Series)
    assert result.empty

def test_fuzzy_match_job_title_close_match():
    title = "busines analist"  # sai chính tả intentional
    assert fuzzy_match_job_title(title) == "Data"



