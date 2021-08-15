from functools import partial, wraps
import re
from time import sleep

import pypandoc


def textile_to_md(text: str):
    """Conver textile formated text to markdown
    """
    text = pypandoc.convert_text(text, to='markdown_github', format='textile')
    return re.sub(r'\\(.)', r'\1', text)


def indent(text, offset=3):
    """Indent text with offset * 4 blank spaces
    """
    def indented_lines():
        for ix, line in enumerate(text.splitlines(True)):
            if ix == 0:
                yield line
            else:
                yield '    ' * offset + line if line.strip() else line
    return ''.join(indented_lines())


def retry(func=None, *, attempts=1, delay=0, exc=(Exception,)):
    """Re-execute decorated function if execution fails

    Execute the function while it fails up to `attempt` times.

    attemps: int
        number of tries, default 1
    delay: float
        timeout between each tries in seconds, default 0
    exc: Collection[Exception]
        collection of exceptions to be caugth
    """
    if func is None:
        return partial(retry, attempts=attempts, delay=delay, exc=exc)

    @wraps(func)
    def retried(*args, **kwargs):
        retry._tries[func.__name__] = 0
        for i in reversed(range(attempts)):
            retry._tries[func.__name__] += 1
            try:
                ret = func(*args, **kwargs)
            except exc:
                if i <= 0:
                    raise
                sleep(delay)
                continue
            else:
                break
        return ret

    retry._tries = {}
    return retried
