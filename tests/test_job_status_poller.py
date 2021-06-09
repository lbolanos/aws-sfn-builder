import random

from aws_sfn_builder import Machine, Runner


def test_runs_job_status_poller(example):
    source = example("job_status_poller")
    sm = Machine.parse(source)
    assert sm.compile() == source
    assert sm.max_pages == 2
    assert sm.states['Submit Job'].parallel_pages == True

    runner = Runner()

    @runner.resource_provider("arn:aws:lambda:REGION:ACCOUNT_ID:function:SubmitJob")
    def submit_job(payload):
        return payload

    @runner.resource_provider("arn:aws:lambda:REGION:ACCOUNT_ID:function:CheckJob")
    def check_job(payload):
        if payload < 30:
            return "FAILED"
        else:
            return "SUCCEEDED"

    final_state, output = runner.run(sm, {'input': 25})
    assert final_state.name in ("Job Failed")
    final_state, output = runner.run(sm, {'input': 40})
    assert final_state.name in ("Consolidator Output")
