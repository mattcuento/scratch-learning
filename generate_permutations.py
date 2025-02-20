def generate_permutations(x: list[int]):
    def generate_permutations_helper(idx: int):
        if idx == len(x) - 1:
            permutations.append(x.copy())
            return

        for j in range(idx, len(x)):
            x[j], x[idx] = x[idx], x[j]
            generate_permutations_helper(idx + 1)
            x[j], x[idx] = x[idx], x[j]

    permutations = []
    generate_permutations_helper(0)
    return permutations


if __name__ == '__main__':
    test_case = [2, 3, 5, 7]
    result = generate_permutations(test_case)
    print(result)
    # assert result
