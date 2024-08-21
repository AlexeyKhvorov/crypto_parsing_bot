def find_first_two_digits(input_string):
    first_two_digits = None
    next_two_digits_after_dot = None

    for i in range(len(input_string) - 1):
        if input_string[i].isdigit() and input_string[i+1].isdigit():
            first_two_digits = input_string[i:i+2]
            break

    dot_found = False
    for i in range(len(input_string)):
        if input_string[i] == '.':
            dot_found = True
        elif dot_found and input_string[i].isdigit() and i < len(input_string) - 1 and input_string[i+1].isdigit():
            next_two_digits_after_dot = input_string[i:i+2]
            break

    return first_two_digits, next_two_digits_after_dot


