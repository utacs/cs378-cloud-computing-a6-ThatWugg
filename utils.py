def is_hexadecimal (char: str):
    ascii: int = ord(char)
    if (ascii >= 48 and ascii <= 57 or # 0-9
       ascii >= 65 and ascii <= 70 or # A-F
       ascii >= 97 and ascii <= 102):  # a-f
        return True
    return False