# transform.py

import pandas as pd
import re
from rapidfuzz import fuzz, process

# ==== Step 1: Chuẩn hóa cột salary ====

def extract_salary_range(s):
    if pd.isna(s):
        return None, None, 'Unknown'

    s = s.lower().replace(',', '').strip()

    if 'usd' in s:
        currency = 'USD'
    elif 'triệu' in s:
        currency = 'VND'
    else:
        currency = 'Other'

    if 'thoả thuận' in s or 'thỏa thuận' in s:
        return None, None, currency

    if 'trên' in s or 'từ' in s or 'from' in s:
        nums = re.findall(r"[\d.]+", s)
        if nums:
            return float(nums[0]), None, currency

    if 'tới' in s or 'to' in s or 'đến' in s:
        nums = re.findall(r"[\d.]+", s)
        if nums:
            return None, float(nums[-1]), currency

    nums = re.findall(r"[\d.]+", s)
    if len(nums) >= 2:
        return float(nums[0]), float(nums[1]), currency
    elif len(nums) == 1:
        return float(nums[0]), float(nums[0]), currency

    return None, None, currency

# ==== Step 2: Tách cột thành phố và quận ====

def build_city_set(df, address_col='address'):
    return set(df[address_col].str.split(':', n=1).str[0].str.strip())

def split_city_district_dynamic(address, city_set, max_pairs=10):
    if not isinstance(address, str):
        return pd.Series()
    parts = [part.strip() for part in address.split(':') if part.strip()]
    result = {}
    city_idx = 1
    i = 0

    while i < len(parts) and city_idx <= max_pairs:
        part = parts[i]
        if part in city_set:
            city_key = f'city_{city_idx}'
            district_key = f'district_{city_idx}'
            result[city_key] = part

            if i + 1 < len(parts) and parts[i + 1] not in city_set:
                result[district_key] = parts[i + 1]
                i += 1
            else:
                result[district_key] = None
            city_idx += 1
        i += 1

    series_result = pd.Series(result)
    return series_result.where(pd.notna(series_result), None)

# ==== Step 3: Chuẩn hóa cột Job Title ====

group_keywords = {
    'Intern': ['thực tập', 'intern'],
    'Software Engineer': ['developer', 'software engineer', 'programmer', 'coder', 'lập trình'],
    'Data Engineer': ['data engineer', 'analystic enginer'],
    'Data': ['data', 'analyst', 'bi', 'business analyst'],
    'DevOps': ['devops', 'sre', 'system admin'],
    'PM': ['project manager', 'scrum master', 'product owner'],
    'IT Support': ['it support', 'helpdesk', 'technical support', 'application support'],
    'Tester': ['qa', 'qc', 'tester', 'kiểm thử'],
    'Other': []
}

# === Chuẩn bị danh sách (group, keyword) flatten để fuzzy search ===
keyword_list = [(group, keyword) for group, keywords in group_keywords.items() for keyword in keywords]

def fuzzy_match_job_title(title, threshold=50):
    title = str(title).lower()
    best_score = 0
    best_group = "Other"
    
    for group, keywords in group_keywords.items():
        for keyword in keywords:
            score = fuzz.ratio(title, keyword) 
            if score > best_score:
                best_score = score
                best_group = group

    if best_score > 0:
        print(f"Fuzzy match: '{title}' -> '{best_group}' with score {best_score}")   
        return best_group if best_score >= threshold else 'Other'

# === Hàm chính: ưu tiên match keyword, nếu không có thì dùng fuzzy ===
def normalize_job_title(title):
    title = str(title).lower()

    # Keyword-based
    for group, keywords in group_keywords.items():
        if any(keyword in title for keyword in keywords):
            return group
        
    # Fallback to fuzzy
    return fuzzy_match_job_title(title)

# group_keywords = {
#     'Intern': ['thực tập', 'intern'],
#     'Software Engineer': ['developer', 'software engineer', 'programmer', 'coder', 'lập trình'],
#     'Data': ['data', 'analyst', 'bi', 'business analyst'],
#     'DevOps': ['devops', 'sre', 'system admin'],
#     'PM': ['project manager', 'scrum master', 'product owner'],
#     'IT Support': ['it support', 'helpdesk', 'technical support', 'application support'],
#     'Tester': ['qa', 'qc', 'tester', 'kiểm thử'],
#     'Other': []
# }

# def normalize_job_title(title):
#     title = str(title).lower()
#     for group, keywords in group_keywords.items():
#         if any(keyword in title for keyword in keywords):
#             return group
#     return 'Other'

# ==== Main Transform Function ====

def transform_data(df):
    # Extract salary fields
    df[['min_salary', 'max_salary', 'unit_currency']] = df['salary'].apply(
        lambda x: pd.Series(extract_salary_range(x))
    )

    # Split address into city/district
    city_set = build_city_set(df)
    city_district_df = df['address'].apply(lambda x: split_city_district_dynamic(x, city_set))
    df = df.join(city_district_df)

    # Normalize job titles
    df['job_group'] = df['job_title'].apply(normalize_job_title)

    df = df.astype(object).where(pd.notna(df), None)

    return df
