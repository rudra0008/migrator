const singleForm = document.getElementById('single-form');
const batchForm = document.getElementById('batch-form');
const singleMessage = document.getElementById('single-message');
const batchMessage = document.getElementById('batch-message');
const jobsTable = document.getElementById('jobs-table');

singleForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  singleMessage.textContent = 'Submitting single migration...';

  const formData = new FormData(singleForm);
  const response = await fetch('/single', { method: 'POST', body: formData });
  const data = await response.json();

  if (!response.ok) {
    singleMessage.textContent = data.error || 'Failed to submit single migration.';
    singleMessage.className = 'error';
    return;
  }

  singleMessage.textContent = `${data.message}: ${data.job_id}`;
  singleMessage.className = 'success';
  singleForm.reset();
  pollJobs();
});

batchForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  batchMessage.textContent = 'Submitting batch migration...';

  const formData = new FormData(batchForm);
  const response = await fetch('/upload', { method: 'POST', body: formData });
  const data = await response.json();

  if (!response.ok) {
    batchMessage.textContent = data.error || 'Failed to submit batch migration.';
    batchMessage.className = 'error';
    return;
  }

  batchMessage.textContent = `${data.message}: ${data.jobs} jobs queued (batch ${data.batch_id}).`;
  batchMessage.className = 'success';
  batchForm.reset();
  pollJobs();
});

async function cancelJob(jobId) {
  const response = await fetch(`/jobs/${jobId}/cancel`, { method: 'POST' });
  const data = await response.json();

  if (!response.ok) {
    alert(data.message || 'Unable to cancel migration.');
  }
  await pollJobs();
}

function escapeHtml(value) {
  return (value ?? '')
    .toString()
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

async function pollJobs() {
  const response = await fetch('/api/jobs');
  const jobs = await response.json();

  jobsTable.innerHTML = '';

  for (const job of jobs) {
    const row = document.createElement('tr');
    const canCancel = job.status === 'queued' || job.status === 'running';

    row.innerHTML = `
      <td>${escapeHtml(job.source_user)}</td>
      <td>${escapeHtml(job.source_server)}</td>
      <td>${escapeHtml(job.destination_user)}</td>
      <td>${escapeHtml(job.destination_server)}</td>
      <td><span class="badge badge-${escapeHtml(job.status)}">${escapeHtml(job.status)}</span></td>
      <td>${escapeHtml(job.error || '')}</td>
      <td>
        ${canCancel ? `<button class="secondary outline" data-job-id="${escapeHtml(job.id)}">Cancel</button>` : '<span>-</span>'}
      </td>
    `;
    jobsTable.appendChild(row);
  }

  document.querySelectorAll('button[data-job-id]').forEach((button) => {
    button.addEventListener('click', () => cancelJob(button.dataset.jobId));
  });
}

setInterval(pollJobs, 2500);
pollJobs();
