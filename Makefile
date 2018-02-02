upload_pypi:
	python setup.py sdist bdist_egg
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
