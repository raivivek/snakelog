from snakelog import __version__
from snakelog.cli import parse_rule_grammar


def test_version():
    assert __version__ == '0.1.0'


def test_parse_rule_grammar_edge_cases():
    """Test that parse_rule_grammar handles malformed lines without crashing."""
    
    # Test cases that previously caused IndexError
    edge_cases = [
        ("output", {"output": []}),
        ("input", {"input": []}),
        ("params", {"params": {}}),
        ("wildcards", {"wildcards": {}}),
        ("resources", {"resources": {}}),
        ("jobid", {"jobid": None}),
        ("log", {"log": ""}),
        ("threads", {"threads": 1}),
    ]
    
    for line, expected in edge_cases:
        result = parse_rule_grammar(line, {})
        assert result == expected, f"Failed for line '{line}': expected {expected}, got {result}"


def test_parse_rule_grammar_valid_cases():
    """Test that parse_rule_grammar works correctly with valid input."""
    
    test_cases = [
        ("output: file1.txt, file2.txt", {"output": ["file1.txt", "file2.txt"]}),
        ("input: input1.txt, input2.txt", {"input": ["input1.txt", "input2.txt"]}),
        ("output: single_file.txt", {"output": ["single_file.txt"]}),
        ("jobid: 123", {"jobid": 123}),
        ("threads: 4", {"threads": 4}),
        ("log: /path/to/log.txt", {"log": "/path/to/log.txt"}),
        ("params: key1=value1, key2=value2", {"params": {"key1": "value1", "key2": "value2"}}),
        ("rule main", {"rule": "main"}),
    ]
    
    for line, expected in test_cases:
        result = parse_rule_grammar(line, {})
        assert result == expected, f"Failed for line '{line}': expected {expected}, got {result}"
