import pytest

from aws_sfn_builder import Machine, ResourceManager, Runner, State


@pytest.mark.parametrize("input_path,expected_resource_input", [
    [None, {"guid": "123-456"}],
    ["$", {"guid": "123-456"}],
    ["$.guid", "123-456"],
])
def test_format_resource_input_returns_filtered_input(input_path, expected_resource_input):
    state = State.parse({
        "InputPath": input_path
    })
    resource_input = state.format_state_input({"guid": "123-456"})
    assert expected_resource_input == resource_input


@pytest.mark.parametrize("result_path,expected_result", [
    [None, "ok"],
    ["$", "ok"],
    ["$.status", {"guid": "123-456", "status": "ok"}]
])
def test_format_result_returns_applied_result(result_path, expected_result):
    state = State.parse({
        "ResultPath": result_path,
    })
    result = state.format_result_selector("ok")
    result = state.format_result({"guid": "123-456"}, result)
    assert expected_result == result


@pytest.mark.parametrize("result_path,expected_result", [
    [{
        "ResultSelector": {
            "ClusterId.$": "$.output.ClusterId",
            "ResourceType.$": "$.resourceType",
            "StaticValue": "foo"
        },
        "ResultPath": "$.EMROutput"
    }, {
        "OtherDataFromInput": {},
        "EMROutput": {
            "ResourceType": "elasticmapreduce",
            "ClusterId": "AKIAIOSFODNN7EXAMPLE",
            "StaticValue": "foo"
        }
    }],
    [
        {
            "ResultSelector": {
                "modifiedPayload": {
                    "body.$": "$.output.SdkHttpMetadata.HttpHeaders.Date",
                    "statusCode.$": "$.resourceType",
                    "requestId.$": "$.output.SdkResponseMetadata.RequestId"
                }
            },
            "ResultPath": "$.mipres_result"
        },
        {
            'OtherDataFromInput': {},
            'mipres_result':
                {
                    'modifiedPayload':
                        {
                            'body': 'Mon, 25 Nov 2019 19:41:29 GMT',
                            'statusCode': 'elasticmapreduce',
                            'requestId': '1234-5678-9012'
                        }
                }
        }
    ]

])
def test_format_result_selector_returns_applied_result(result_path, expected_result):
    state = State.parse(result_path)
    input = {
        "resourceType": "elasticmapreduce",
        "resource": "createCluster.sync",
        "output": {
            "SdkHttpMetadata": {
                "HttpHeaders": {
                    "Content-Length": "1112",
                    "Content-Type": "application/x-amz-JSON-1.1",
                    "Date": "Mon, 25 Nov 2019 19:41:29 GMT",
                    "x-amzn-RequestId": "1234-5678-9012"
                },
                "HttpStatusCode": 200
            },
            "SdkResponseMetadata": {
                "RequestId": "1234-5678-9012"
            },
            "ClusterId": "AKIAIOSFODNN7EXAMPLE"
        }
    }
    result = state.format_result_selector(input)
    result_final = state.format_result({"OtherDataFromInput": {}}, result)
    assert expected_result == result_final


@pytest.mark.parametrize("result_path,expected_result", [
    [{
        "ResultSelector": {
            "modifiedPayload": {
                "body.$": "$.Payload.body",
                "statusCode.$": "$.Payload.statusCode",
                "requestId.$": "$.SdkResponseMetadata.RequestId"
            }
        },
        "ResultPath": "$.TaskResult",
        "OutputPath": "$.TaskResult.modifiedPayload"
    }, {
        "body": "hello, world!",
        "statusCode": "200",
        "requestId": "88fba57b-adbe-467f-abf4-daca36fc9028"
    }]

])
def test_format_result_all_applied_result(result_path, expected_result):
    state = State.parse(result_path)
    output = {
        "ExecutedVersion": "$LATEST",
        "Payload": {
            "statusCode": "200",
            "body": "hello, world!"
        },
        "SdkHttpMetadata": {
            "HttpHeaders": {
                "Connection": "keep-alive",
                "Content-Length": "43",
                "Content-Type": "application/json",
                "Date": "Thu, 16 Apr 2020 17:58:15 GMT",
                "X-Amz-Executed-Version": "$LATEST",
                "x-amzn-Remapped-Content-Length": "0",
                "x-amzn-RequestId": "88fba57b-adbe-467f-abf4-daca36fc9028",
                "X-Amzn-Trace-Id": "root=1-5e989cb6-90039fd8971196666b022b62;sampled=0"
            },
            "HttpStatusCode": 200
        },
        "SdkResponseMetadata": {
            "RequestId": "88fba57b-adbe-467f-abf4-daca36fc9028"
        },
        "StatusCode": 200
    }
    next_state, result = state.get_output({"OtherDataFromInput": {}}, output)
    assert expected_result == result


@pytest.mark.parametrize("result_path,expected_result", [
    [{
        "InputPath": "$.library",
        "Parameters": {
            "staticValue": "Just a string",
            "catalog": {
                "myFavoriteMovie.$": "$.movies[0]"
            }
        }
    }, {
        "staticValue": "Just a string",
        "catalog": {
            "myFavoriteMovie": {
                "genre": "crime",
                "director": "Quentin Tarantino",
                "title": "Reservoir Dogs",
                "year": 1992
            }
        }
    }]

])
def test_format_input_all_applied_result(result_path, expected_result):
    state = State.parse(result_path)
    input = {
        "version": 4,
        "library": {
            "movies": [
                {
                    "genre": "crime",
                    "director": "Quentin Tarantino",
                    "title": "Reservoir Dogs",
                    "year": 1992
                },
                {
                    "genre": "action",
                    "director": "Brian De Palma",
                    "title": "Mission: Impossible",
                    "year": 1996,
                    "staring": [
                        "Tom Cruise"
                    ]
                }
            ],
            "metadata": {
                "lastUpdated": "2020-05-27T08:00:00.000Z"
            }
        }
    }
    result = state.get_input(input)
    assert expected_result == result


@pytest.mark.parametrize("output_path,expected_state_output", [
    [None, {"guid": "123-456"}],
    ["$", {"guid": "123-456"}],
    ["$.guid", "123-456"],
])
def test_format_state_output_returns_filtered_output(output_path, expected_state_output):
    state = State.parse({
        "OutputPath": output_path
    })
    state_output = state.format_state_output({"guid": "123-456"})
    assert expected_state_output == state_output


def test_executes_hello_world_state(example):
    hello_world_state = Machine.parse(example("hello_world")).start_at_state
    assert isinstance(hello_world_state, State)

    resources = ResourceManager(providers={
        "arn:aws:lambda:us-east-1:123456789012:function:HelloWorld": lambda x: "Hello, world!"
    })
    next_state, output = hello_world_state.execute({}, resource_resolver=resources)
    assert output == "Hello, world!"


def test_runs_hello_world_machine(example):
    sm = Machine.parse(example("hello_world"))

    runner = Runner(resources=ResourceManager(providers={
        "arn:aws:lambda:us-east-1:123456789012:function:HelloWorld": lambda x: "Hello, world!"
    }))

    assert runner.run(sm) == (sm.start_at_state, "Hello, world!")


def test_input_passed_to_next_task():
    sm = Machine.parse([
        {
            "InputPath": "$.first_input",
            "ResultPath": "$.first_output",
            "Resource": "MultiplierByTwo",
        },
        {
            "InputPath": "$.first_output",
            "ResultPath": "$.second_output",
            "Resource": "MultiplierByThree",
        },
        {
            "Resource": "Validator",
        },
    ])

    runner = Runner()
    runner.resource_provider("MultiplierByTwo")(lambda x: x * 2)
    runner.resource_provider("MultiplierByThree")(lambda x: x * 3)

    @runner.resource_provider("Validator")
    def validate_input(input):
        assert input == {
            "first_input": 1111,
            "first_output": 2222,
            "second_output": 6666,
        }
        # NB!
        return input

    final_state, output = runner.run(sm, input={"first_input": 1111})
    assert output == {
        "first_input": 1111,
        "first_output": 2222,
        "second_output": 6666,
    }


@pytest.mark.parametrize("input,expected_output", [
    [{}, {}],
    [{"x": 1}, {"x": 1}],
])
def test_executes_wait_state(input, expected_output):
    wait = State.parse({
        "Type": "Wait",
        "Seconds": 10,
        "Next": "NextState",
    })
    next_state, output = wait.execute(input=input)
    assert next_state == "NextState"
    assert expected_output == output


def test_executes_fail_state():
    fail = State.parse({
        "Type": "Fail",
        "Error": "ErrorA",
        "Cause": "Kaiju attack",
    })
    # TODO No idea what should be the next state or output of fail state.
    # TODO Should it just raise an exception?
    next_state, output = fail.execute(input=input)
    assert next_state is None
