# for task 1
def is_valid_line1 (line: str):
    # check comma count
    comma_count: int = 0
    first_comma_idx: int = None # keep track of driver id index
    second_comma_idx: int = None # keep track of end of driver id
    for idx in range(len(line)):
        if line[idx] == ',':
            comma_count += 1
            if comma_count == 1:
                first_comma_idx = idx
            if comma_count == 2:
                second_comma_idx = idx
    if comma_count != 16:
        return False

    # ensure that length of md5sums are correct
    if first_comma_idx != 32: return False
    if second_comma_idx != 65: return False
    # ensure that contents of md5sums are correct
    # by checking they are hexadecimal
    for idx in range(first_comma_idx):
        if not is_hexadecimal(line[idx]):
            return False
        
    for idx in range(first_comma_idx + 1, second_comma_idx):
        if not is_hexadecimal(line[idx]):
            return False
    
    return True
        
            
def is_hexadecimal (char: str):
    ascii: int = ord(char)
    if (ascii >= 48 and ascii <= 57 or # 0-9
       ascii >= 65 and ascii <= 70 or # A-F
       ascii >= 97 and ascii <= 102):  # a-f
        return True
    return False