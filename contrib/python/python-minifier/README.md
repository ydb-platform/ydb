# Python Minifier

Transforms Python source code into its most compact representation.

[Try it out!](https://python-minifier.com)

python-minifier currently supports Python 2.7 and Python 3.3 to 3.14. Previous releases supported Python 2.6.

* [PyPI](https://pypi.org/project/python-minifier/)
* [Documentation](https://dflook.github.io/python-minifier/)
* [Issues](https://github.com/dflook/python-minifier/issues)

As an example, the following python source:

```python
def handler(event, context):
    l.info(event)
    try:
        i_token = hashlib.new('md5', (event['RequestId'] + event['StackId']).encode()).hexdigest()
        props = event['ResourceProperties']

        if event['RequestType'] == 'Create':
            event['PhysicalResourceId'] = 'None'
            event['PhysicalResourceId'] = create_cert(props, i_token)
            add_tags(event['PhysicalResourceId'], props)
            validate(event['PhysicalResourceId'], props)

            if wait_for_issuance(event['PhysicalResourceId'], context):
                event['Status'] = 'SUCCESS'
                return send(event)
            else:
                return reinvoke(event, context)

        elif event['RequestType'] == 'Delete':
            if event['PhysicalResourceId'] != 'None':
                acm.delete_certificate(CertificateArn=event['PhysicalResourceId'])
            event['Status'] = 'SUCCESS'
            return send(event)

        elif event['RequestType'] == 'Update':

            if replace_cert(event):
                event['PhysicalResourceId'] = create_cert(props, i_token)
                add_tags(event['PhysicalResourceId'], props)
                validate(event['PhysicalResourceId'], props)

                if not wait_for_issuance(event['PhysicalResourceId'], context):
                    return reinvoke(event, context)
            else:
                if 'Tags' in event['OldResourceProperties']:
                    acm.remove_tags_from_certificate(CertificateArn=event['PhysicalResourceId'],
                                                     Tags=event['OldResourceProperties']['Tags'])

                add_tags(event['PhysicalResourceId'], props)

            event['Status'] = 'SUCCESS'
            return send(event)
        else:
            raise RuntimeError('Unknown RequestType')

    except Exception as ex:
        l.exception('')
        event['Status'] = 'FAILED'
        event['Reason'] = str(ex)
        return send(event)
```

Becomes:

```python
def handler(event,context):
	L='OldResourceProperties';K='Tags';J='None';H='SUCCESS';G='RequestType';E='Status';D=context;B='PhysicalResourceId';A=event;l.info(A)
	try:
		F=hashlib.new('md5',(A['RequestId']+A['StackId']).encode()).hexdigest();C=A['ResourceProperties']
		if A[G]=='Create':
			A[B]=J;A[B]=create_cert(C,F);add_tags(A[B],C);validate(A[B],C)
			if wait_for_issuance(A[B],D):A[E]=H;return send(A)
			else:return reinvoke(A,D)
		elif A[G]=='Delete':
			if A[B]!=J:acm.delete_certificate(CertificateArn=A[B])
			A[E]=H;return send(A)
		elif A[G]=='Update':
			if replace_cert(A):
				A[B]=create_cert(C,F);add_tags(A[B],C);validate(A[B],C)
				if not wait_for_issuance(A[B],D):return reinvoke(A,D)
			else:
				if K in A[L]:acm.remove_tags_from_certificate(CertificateArn=A[B],Tags=A[L][K])
				add_tags(A[B],C)
			A[E]=H;return send(A)
		else:raise RuntimeError('Unknown RequestType')
	except Exception as I:l.exception('');A[E]='FAILED';A['Reason']=str(I);return send(A)
```

## Why?

AWS Cloudformation templates may have AWS lambda function source code embedded in them, but only if the function is less
than 4KiB. I wrote this package so I could write python normally and still embed the module in a template.

## Installation

To install python-minifier use pip:

```bash
$ pip install python-minifier
```

Note that python-minifier depends on the python interpreter for parsing source code,
and outputs source code compatible with the version of the interpreter it is run with.

This means that if you minify code written for Python 3.11 using python-minifier running with Python 3.12,
the minified code may only run with Python 3.12.

python-minifier runs with and can minify code written for Python 2.7 and Python 3.3 to 3.14.

## Usage

To minify a source file, and write the minified module to stdout:

```bash
$ pyminify hello.py
```

There is also an API. The same example would look like:

```python
import python_minifier

with open('hello.py') as f:
    print(python_minifier.minify(f.read()))
```

Documentation is available at [dflook.github.io/python-minifier/](https://dflook.github.io/python-minifier/)

## License

Available under the MIT License. Full text is in the [LICENSE](LICENSE) file.

Copyright (c) 2024 Daniel Flook
